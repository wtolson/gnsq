from __future__ import with_statement

import struct
import json
import pytest

from gnsq import Nsqd, Message, states, errors
from gnsq import protocol as nsq
from gnsq.stream.stream import SSLSocket, DefalteSocket, SnappySocket

from mock_server import mock_server
from integration_server import NsqdIntegrationServer


def mock_response(frame_type, data):
    body_size = 4 + len(data)
    body_size_packed = struct.pack('>l', body_size)
    frame_type_packed = struct.pack('>l', frame_type)
    return body_size_packed + frame_type_packed + data


def mock_response_message(timestamp, attempts, id, body):
    timestamp_packed = struct.pack('>q', timestamp)
    attempts_packed = struct.pack('>h', attempts)
    id = "%016d" % id
    data = timestamp_packed + attempts_packed + id + body
    return mock_response(nsq.FRAME_TYPE_MESSAGE, data)


def test_connection():
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == '  V2'
        assert socket.recv(1) == ''

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        assert conn.state == states.INIT

        conn.connect()
        assert conn.state == states.CONNECTED

        conn.connect()
        assert conn.state == states.CONNECTED

        conn.close_stream()
        assert conn.state == states.DISCONNECTED


def test_disconnected():
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == '  V2'
        assert socket.recv(1) == ''

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()
        conn.close_stream()
        assert conn.state == states.DISCONNECTED

        with pytest.raises(errors.NSQSocketError):
            conn.nop()

        with pytest.raises(errors.NSQSocketError):
            conn.read_response()


@pytest.mark.parametrize('body', [
    'hello world',
    '',
    '{"some": "json data"}',
])
def test_read(body):
    @mock_server
    def handle(socket, address):
        socket.send(struct.pack('>l', len(body)))
        socket.send(body)

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()

        assert conn._read_response() == body
        conn.close_stream()


def test_identify():
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == '  V2'
        assert socket.recv(9) == 'IDENTIFY\n'

        size = nsq.unpack_size(socket.recv(4))
        data = json.loads(socket.recv(size))

        assert 'gnsq' in data['user_agent']
        socket.send(mock_response(nsq.FRAME_TYPE_RESPONSE, 'OK'))

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()

        assert conn.identify() is None


def test_negotiation():
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == '  V2'
        assert socket.recv(9) == 'IDENTIFY\n'

        size = nsq.unpack_size(socket.recv(4))
        data = json.loads(socket.recv(size))

        assert 'gnsq' in data['user_agent']
        resp = json.dumps({'test': 42})
        socket.send(mock_response(nsq.FRAME_TYPE_RESPONSE, resp))

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()

        assert conn.identify()['test'] == 42


@pytest.mark.parametrize('command,args,resp', [
    ('subscribe', ('topic', 'channel'), 'SUB topic channel\n'),
    ('subscribe', ('foo', 'bar'), 'SUB foo bar\n'),
    ('ready', (0,), 'RDY 0\n'),
    ('ready', (1,), 'RDY 1\n'),
    ('ready', (42,), 'RDY 42\n'),
    ('finish', ('0000000000000000',), 'FIN 0000000000000000\n'),
    ('finish', ('deadbeafdeadbeaf',), 'FIN deadbeafdeadbeaf\n'),
    ('requeue', ('0000000000000000',), 'REQ 0000000000000000 0\n'),
    ('requeue', ('deadbeafdeadbeaf', 0), 'REQ deadbeafdeadbeaf 0\n'),
    ('requeue', ('deadbeafdeadbeaf', 42), 'REQ deadbeafdeadbeaf 42\n'),
    ('touch', ('0000000000000000',), 'TOUCH 0000000000000000\n'),
    ('touch', ('deadbeafdeadbeaf',), 'TOUCH deadbeafdeadbeaf\n'),
    ('close', (), 'CLS\n'),
    ('nop', (), 'NOP\n'),
])
def test_command(command, args, resp):
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == '  V2'
        assert socket.recv(len(resp)) == resp

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()
        getattr(conn, command)(*args)


def test_publish():
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == '  V2'
        assert socket.recv(10) == 'PUB topic\n'

        assert nsq.unpack_size(socket.recv(4)) == 3
        assert socket.recv(3) == 'sup'

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()
        conn.publish('topic', 'sup')


def test_multipublish():
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == '  V2'
        assert socket.recv(11) == 'MPUB topic\n'

        size = nsq.unpack_size(socket.recv(4))
        data = socket.recv(size)

        head, data = data[:4], data[4:]
        assert nsq.unpack_size(head) == 2

        for _ in xrange(2):
            head, data = data[:4], data[4:]
            assert nsq.unpack_size(head) == 3

            head, data = data[:3], data[3:]
            assert head == 'sup'

        assert data == ''

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()
        conn.multipublish('topic', ['sup', 'sup'])


@pytest.mark.parametrize('error,error_class,fatal', [
    ('E_INVALID', errors.NSQInvalid, True),
    ('E_BAD_BODY', errors.NSQBadBody, True),
    ('E_BAD_TOPIC', errors.NSQBadTopic, True),
    ('E_BAD_CHANNEL', errors.NSQBadChannel, True),
    ('E_BAD_MESSAGE', errors.NSQBadMessage, True),
    ('E_PUT_FAILED', errors.NSQPutFailed, True),
    ('E_PUB_FAILED', errors.NSQPubFailed, True),
    ('E_MPUB_FAILED', errors.NSQMPubFailed, True),
    ('E_FIN_FAILED', errors.NSQFinishFailed, False),
    ('E_REQ_FAILED', errors.NSQRequeueFailed, False),
    ('E_TOUCH_FAILED', errors.NSQTouchFailed, False),
    ('unknown error', errors.NSQException, True),
])
def test_error(error, error_class, fatal):
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == '  V2'
        socket.send(mock_response(nsq.FRAME_TYPE_ERROR, error))

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()

        frame, resp = conn.read_response()
        assert frame == nsq.FRAME_TYPE_ERROR
        assert isinstance(resp, error_class)
        assert conn.is_connected != fatal


def test_hashing():
    conn1 = Nsqd('localhost', 1337)
    conn2 = Nsqd('localhost', 1337)
    assert conn1 == conn2
    assert not (conn1 < conn2)
    assert not (conn2 < conn1)

    test = {conn1: True}
    assert conn2 in test


def test_sync_receive_messages():
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == '  V2'
        assert socket.recv(9) == 'IDENTIFY\n'

        size = nsq.unpack_size(socket.recv(4))
        data = json.loads(socket.recv(size))

        assert isinstance(data, dict)
        socket.send(mock_response(nsq.FRAME_TYPE_RESPONSE, 'OK'))

        msg = 'SUB topic channel\n'
        assert socket.recv(len(msg)) == msg
        socket.send(mock_response(nsq.FRAME_TYPE_RESPONSE, 'OK'))

        for i in xrange(10):
            assert socket.recv(6) == 'RDY 1\n'

            body = json.dumps({'data': {'test_key': i}})
            ts = i * 1000 * 1000
            socket.send(mock_response_message(ts, i, i, body))

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()

        assert conn.identify() is None

        conn.subscribe('topic', 'channel')
        frame, data = conn.read_response()

        assert frame == nsq.FRAME_TYPE_RESPONSE
        assert data == 'OK'

        for i in xrange(10):
            conn.ready(1)
            frame, msg = conn.read_response()

            assert frame == nsq.FRAME_TYPE_MESSAGE
            assert isinstance(msg, Message)
            assert msg.timestamp == i * 1000 * 1000
            assert msg.id == '%016d' % i
            assert msg.attempts == i
            assert json.loads(msg.body)['data']['test_key'] == i


def test_sync_heartbeat():
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == '  V2'
        socket.send(mock_response(nsq.FRAME_TYPE_RESPONSE, '_heartbeat_'))
        assert socket.recv(4) == 'NOP\n'

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()

        frame, data = conn.read_response()
        assert frame == nsq.FRAME_TYPE_RESPONSE
        assert data == '_heartbeat_'


@pytest.mark.slow
def test_tls():
    with NsqdIntegrationServer() as server:
        conn = Nsqd(
            address=server.address,
            tcp_port=server.tcp_port,
            tls_v1=True,
            tls_options={
                'keyfile': server.tls_key,
                'certfile': server.tls_cert,
            }
        )
        conn.connect()
        assert conn.state == states.CONNECTED

        resp = conn.identify()
        assert isinstance(resp, dict)
        assert resp['tls_v1'] is True
        assert isinstance(conn.stream.socket, SSLSocket)

        frame, data = conn.read_response()
        assert frame == nsq.FRAME_TYPE_RESPONSE
        assert data == 'OK'

        conn.publish('topic', 'sup')
        frame, data = conn.read_response()
        assert frame == nsq.FRAME_TYPE_RESPONSE
        assert data == 'OK'

        conn.close()


@pytest.mark.slow
def test_deflate():
    with NsqdIntegrationServer() as server:
        conn = Nsqd(
            address=server.address,
            tcp_port=server.tcp_port,
            deflate=True
        )
        conn.connect()
        assert conn.state == states.CONNECTED

        resp = conn.identify()
        assert isinstance(resp, dict)
        assert resp['deflate'] is True
        assert isinstance(conn.stream.socket, DefalteSocket)

        frame, data = conn.read_response()
        assert frame == nsq.FRAME_TYPE_RESPONSE
        assert data == 'OK'

        conn.close()


@pytest.mark.slow
def test_snappy():
    with NsqdIntegrationServer() as server:
        conn = Nsqd(
            address=server.address,
            tcp_port=server.tcp_port,
            snappy=True
        )
        conn.connect()
        assert conn.state == states.CONNECTED

        resp = conn.identify()
        assert isinstance(resp, dict)
        assert resp['snappy'] is True
        assert isinstance(conn.stream.socket, SnappySocket)

        frame, data = conn.read_response()
        assert frame == nsq.FRAME_TYPE_RESPONSE
        assert data == 'OK'

        conn.close()


@pytest.mark.slow
def test_cls_error():
    with NsqdIntegrationServer() as server:
        conn = Nsqd(address=server.address, tcp_port=server.tcp_port)

        conn.connect()
        assert conn.state == states.CONNECTED

        conn.close()
        frame, error = conn.read_response()
        assert frame == nsq.FRAME_TYPE_ERROR
        assert isinstance(error, errors.NSQInvalid)
