from __future__ import division, with_statement

import multiprocessing
import os

import pytest
import gevent

from six.moves import range
from gnsq import NsqdHTTPClient, Reader
from gnsq.errors import NSQSocketError

from integration_server import NsqdIntegrationServer


SLOW_TIMEOUT = int(os.environ.get('SLOW_TIMEOUT', '10'), 10)


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
@pytest.mark.timeout(SLOW_TIMEOUT)
def test_max_concurrency():
    server1 = NsqdIntegrationServer()
    server2 = NsqdIntegrationServer()

    with server1, server2:
        class Accounting(object):
            count = 0
            total = 100
            concurrency = 0
            error = None

        for server in (server1, server2):
            conn = NsqdHTTPClient(server.address, server.http_port)

            for _ in range(Accounting.total // 2):
                conn.publish('test', b'danger zone!')

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
