import pytest
from gnsq import Nsqd, Reader
from gnsq.errors import NSQSocketError
from integration_server import IntegrationNsqdServer


@pytest.mark.slow
def test_messages():
    with IntegrationNsqdServer() as server:

        class Accounting(object):
            count = 0
            total = 500
            error = None

        conn = Nsqd(
            address=server.address,
            tcp_port=server.tcp_port,
            http_port=server.http_port,
        )

        for _ in xrange(Accounting.total):
            conn.publish_http('test', 'danger zone!')

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
            assert message.body == 'danger zone!'

            Accounting.count += 1
            if Accounting.count == Accounting.total:
                reader.close()

        try:
            reader.start()
        except NSQSocketError:
            pass

        if Accounting.error:
            raise Accounting.error

        assert Accounting.count == Accounting.total
