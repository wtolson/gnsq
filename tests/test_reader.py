from __future__ import division, with_statement

import multiprocessing
import time
import pytest
import gevent

from six.moves import range
from gnsq import Nsqd, Reader, states
from gnsq.errors import NSQSocketError

from integration_server import (
    with_all,
    LookupdIntegrationServer,
    NsqdIntegrationServer
)


def test_basic():
    with pytest.raises(ValueError):
        Reader('test', 'test')

    with pytest.raises(TypeError):
        Reader(
            topic='test',
            channel='test',
            nsqd_tcp_addresses=None,
            lookupd_http_addresses='http://localhost:4161/',
        )

    with pytest.raises(TypeError):
        Reader(
            topic='test',
            channel='test',
            nsqd_tcp_addresses='localhost:4150',
            lookupd_http_addresses=None,
        )

    def message_handler(reader, message):
        pass

    reader = Reader(
        topic='test',
        channel='test',
        name='test',
        max_concurrency=-1,
        nsqd_tcp_addresses='localhost:4150',
        lookupd_http_addresses='http://localhost:4161/',
        message_handler=message_handler
    )

    assert reader.name == 'test'
    assert reader.max_concurrency == multiprocessing.cpu_count()
    assert len(reader.on_message.receivers) == 1

    assert isinstance(reader.nsqd_tcp_addresses, set)
    assert len(reader.nsqd_tcp_addresses) == 1

    assert isinstance(reader.lookupds, list)
    assert len(reader.lookupds) == 1


@pytest.mark.slow
@pytest.mark.timeout(60)
def test_messages():
    with NsqdIntegrationServer() as server:

        class Accounting(object):
            count = 0
            total = 500
            error = None

        conn = Nsqd(
            address=server.address,
            tcp_port=server.tcp_port,
            http_port=server.http_port,
        )

        for _ in range(Accounting.total):
            conn.publish_http('test', b'danger zone!')

        reader = Reader(
            topic='test',
            channel='test',
            nsqd_tcp_addresses=[server.tcp_address],
            max_in_flight=100,
        )

        @reader.on_exception.connect
        def error_handler(reader, message, error):
            if isinstance(error, NSQSocketError):
                return
            Accounting.error = error
            reader.close()

        @reader.on_message.connect
        def handler(reader, message):
            assert message.body == b'danger zone!'

            Accounting.count += 1
            if Accounting.count == Accounting.total:
                assert not reader.is_starved
                reader.close()

        reader.start()

        if Accounting.error:
            raise Accounting.error

        assert Accounting.count == Accounting.total


@pytest.mark.slow
@pytest.mark.timeout(60)
def test_max_concurrency():
    server1 = NsqdIntegrationServer()
    server2 = NsqdIntegrationServer()

    @with_all(server1, server2)
    def _(server1, server2):
        class Accounting(object):
            count = 0
            total = 100
            concurrency = 0
            error = None

        for server in (server1, server2):
            conn = Nsqd(
                address=server.address,
                tcp_port=server.tcp_port,
                http_port=server.http_port,
            )

            for _ in range(Accounting.total // 2):
                conn.publish_http('test', b'danger zone!')

        reader = Reader(
            topic='test',
            channel='test',
            nsqd_tcp_addresses=[
                server1.tcp_address,
                server2.tcp_address,
            ],
            max_in_flight=5,
            max_concurrency=1,
        )

        @reader.on_exception.connect
        def error_handler(reader, message, error):
            if isinstance(error, NSQSocketError):
                return
            Accounting.error = error
            reader.close()

        @reader.on_message.connect
        def handler(reader, message):
            assert message.body == b'danger zone!'
            assert Accounting.concurrency == 0

            Accounting.concurrency += 1
            gevent.sleep()
            Accounting.concurrency -= 1

            Accounting.count += 1
            if Accounting.count == Accounting.total:
                reader.close()

        reader.start()

        if Accounting.error:
            raise Accounting.error

        assert Accounting.count == Accounting.total


@pytest.mark.slow
@pytest.mark.timeout(60)
def test_lookupd():
    with LookupdIntegrationServer() as lookupd_server:
        server1 = NsqdIntegrationServer(lookupd=lookupd_server.tcp_address)
        server2 = NsqdIntegrationServer(lookupd=lookupd_server.tcp_address)

        @with_all(server1, server2)
        def _(server1, server2):
            class Accounting(object):
                count = 0
                total = 500
                concurrency = 0
                error = None

            for server in (server1, server2):
                conn = Nsqd(
                    address=server.address,
                    tcp_port=server.tcp_port,
                    http_port=server.http_port,
                )

                for _ in range(Accounting.total // 2):
                    conn.publish_http('test', b'danger zone!')

            reader = Reader(
                topic='test',
                channel='test',
                lookupd_http_addresses=lookupd_server.http_address,
                max_in_flight=32,
            )

            @reader.on_exception.connect
            def error_handler(reader, message, error):
                if isinstance(error, NSQSocketError):
                    return
                Accounting.error = error
                reader.close()

            @reader.on_message.connect
            def handler(reader, message):
                assert message.body == b'danger zone!'

                Accounting.count += 1
                if Accounting.count == Accounting.total:
                    reader.close()

            gevent.sleep(0.1)
            reader.start()

            if Accounting.error:
                raise Accounting.error

            assert Accounting.count == Accounting.total


@pytest.mark.slow
@pytest.mark.timeout(60)
def test_backoff():
    with NsqdIntegrationServer() as server:
        conn = Nsqd(
            address=server.address,
            tcp_port=server.tcp_port,
            http_port=server.http_port,
        )

        for _ in range(500):
            conn.publish_http('test', 'danger zone!')

        reader = Reader(
            topic='test',
            channel='test',
            nsqd_tcp_addresses=[server.tcp_address],
            max_in_flight=100,
            message_handler=lambda reader, message: None
        )

        reader.start(block=False)
        reader.start_backoff()

        assert reader.state == states.THROTTLED
        assert reader.total_in_flight_or_ready <= 1

        reader.complete_backoff()
        assert reader.state == states.RUNNING


def test_no_handlers():
    reader = Reader('test', 'test', 'localhost:4150')
    with pytest.raises(RuntimeError):
        reader.start(block=False)


def test_random_ready_conn():
    conn_1 = Nsqd('nsq1')
    conn_2 = Nsqd('nsq2')
    reader = Reader('test', 'test', 'localhost:4150')

    reader.max_in_flight = 2
    reader.conns = [conn_2]
    reader.last_random_ready = 0
    assert reader.random_ready_conn(conn_1) is conn_1
    assert reader.last_random_ready == 0

    reader.max_in_flight = 1
    reader.conns = [conn_2, conn_2]
    reader.last_random_ready = last_random_ready = time.time()
    assert reader.random_ready_conn(conn_1) is conn_1
    assert reader.last_random_ready == last_random_ready

    reader.max_in_flight = 1
    reader.conns = [conn_2, conn_2]
    reader.last_random_ready = 0
    assert reader.random_ready_conn(conn_1) is conn_2
    assert reader.last_random_ready != 0

    reader.max_in_flight = 1
    reader.conns = [conn_2, conn_2]
    reader.last_random_ready = 0
    conn_2.ready_count = 1
    assert reader.random_ready_conn(conn_1) is conn_1
    assert reader.last_random_ready == 0
