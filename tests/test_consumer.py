from __future__ import division, with_statement

import os

import pytest
import gevent

from gnsq import NsqdHTTPClient, Consumer, states
from gnsq.errors import NSQSocketError

from integration_server import LookupdIntegrationServer, NsqdIntegrationServer


SLOW_TIMEOUT = int(os.environ.get('SLOW_TIMEOUT', '10'), 10)


def test_basic():
    with pytest.raises(ValueError):
        Consumer('test', 'test')

    with pytest.raises(TypeError):
        Consumer(
            topic='test',
            channel='test',
            nsqd_tcp_addresses=None,
            lookupd_http_addresses='http://localhost:4161/',
        )

    with pytest.raises(TypeError):
        Consumer(
            topic='test',
            channel='test',
            nsqd_tcp_addresses='localhost:4150',
            lookupd_http_addresses=None,
        )

    def message_handler(consumer, message):
        pass

    consumer = Consumer(
        topic='test',
        channel='test',
        name='test',
        nsqd_tcp_addresses='localhost:4150',
        lookupd_http_addresses='http://localhost:4161/',
        message_handler=message_handler
    )

    assert consumer.name == 'test'
    assert len(consumer.on_message.receivers) == 1

    assert isinstance(consumer.nsqd_tcp_addresses, set)
    assert len(consumer.nsqd_tcp_addresses) == 1

    assert isinstance(consumer.lookupds, list)
    assert len(consumer.lookupds) == 1


@pytest.mark.slow
@pytest.mark.timeout(SLOW_TIMEOUT)
def test_messages():
    with NsqdIntegrationServer() as server:

        class Accounting(object):
            count = 0
            total = 500
            error = None

        conn = NsqdHTTPClient(server.address, server.http_port)
        for _ in range(Accounting.total):
            conn.publish('test', b'danger zone!')

        consumer = Consumer(
            topic='test',
            channel='test',
            nsqd_tcp_addresses=[server.tcp_address],
            max_in_flight=100,
        )

        @consumer.on_exception.connect
        def error_handler(consumer, message, error):
            if isinstance(error, NSQSocketError):
                return
            Accounting.error = error
            consumer.close()

        @consumer.on_message.connect
        def handler(consumer, message):
            assert message.body == b'danger zone!'

            Accounting.count += 1
            if Accounting.count == Accounting.total:
                consumer.close()

        consumer.start()

        if Accounting.error:
            raise Accounting.error

        assert Accounting.count == Accounting.total


@pytest.mark.slow
@pytest.mark.timeout(SLOW_TIMEOUT)
def test_lookupd():
    with LookupdIntegrationServer() as lookupd_server:
        server1 = NsqdIntegrationServer(lookupd=lookupd_server.tcp_address)
        server2 = NsqdIntegrationServer(lookupd=lookupd_server.tcp_address)

        with server1, server2:
            class Accounting(object):
                count = 0
                total = 500
                concurrency = 0
                error = None

            for server in (server1, server2):
                conn = NsqdHTTPClient(server.address, server.http_port)

                for _ in range(Accounting.total // 2):
                    conn.publish('test', b'danger zone!')

            consumer = Consumer(
                topic='test',
                channel='test',
                lookupd_http_addresses=lookupd_server.http_address,
                max_in_flight=32,
            )

            @consumer.on_exception.connect
            def error_handler(consumer, message, error):
                if isinstance(error, NSQSocketError):
                    return
                Accounting.error = error
                consumer.close()

            @consumer.on_message.connect
            def handler(consumer, message):
                assert message.body == b'danger zone!'

                Accounting.count += 1
                if Accounting.count == Accounting.total:
                    consumer.close()

            gevent.sleep(0.1)
            consumer.start()

            if Accounting.error:
                raise Accounting.error

            assert Accounting.count == Accounting.total


@pytest.mark.slow
@pytest.mark.timeout(SLOW_TIMEOUT)
def test_backoff():
    with NsqdIntegrationServer() as server:
        conn = NsqdHTTPClient(server.address, server.http_port)

        for _ in range(500):
            conn.publish('test', 'danger zone!')

        consumer = Consumer(
            topic='test',
            channel='test',
            nsqd_tcp_addresses=[server.tcp_address],
            max_in_flight=100,
            message_handler=lambda consumer, message: None
        )

        consumer.start(block=False)
        consumer._redistributed_ready_event.wait()

        conn = next(iter(consumer._connections))
        consumer._message_backoffs[conn].failure()
        consumer._message_backoffs[conn].failure()
        consumer._start_backoff(conn)
        consumer._redistribute_ready_state()

        assert consumer._connections[conn] == states.BACKOFF
        assert consumer.total_ready_count == 0

        consumer._start_throttled(conn)
        consumer._redistribute_ready_state()
        consumer._redistribute_ready_state()

        assert consumer._connections[conn] == states.THROTTLED
        assert consumer.total_ready_count == 1

        consumer._message_backoffs[conn].success()
        consumer._complete_backoff(conn)
        consumer._redistribute_ready_state()

        assert consumer._connections[conn] == states.BACKOFF
        assert consumer.total_ready_count == 0

        consumer._start_throttled(conn)
        consumer._redistribute_ready_state()

        assert consumer._connections[conn] == states.THROTTLED
        assert consumer.total_ready_count == 1

        consumer._message_backoffs[conn].success()
        consumer._complete_backoff(conn)
        consumer._redistribute_ready_state()

        assert consumer._connections[conn] == states.RUNNING
        assert consumer.total_ready_count == 100


def test_no_handlers():
    consumer = Consumer('test', 'test', 'localhost:4150')
    with pytest.raises(RuntimeError):
        consumer.start(block=False)
