from __future__ import with_statement

import sys
import struct
import json
import ssl
import pytest
import gevent
import six

from six.moves import range
from itertools import product
from gnsq import Nsqd, Message, states, errors
from gnsq import protocol as nsq
from gnsq.stream.stream import SSLSocket, DefalteSocket, SnappySocket

from mock_server import mock_server
from integration_server import NsqdIntegrationServer


BAD_GEVENT = all([
    sys.version_info > (2, 7, 8),
    sys.version_info < (3, 0),
    gevent.version_info < (1, 0, 2),
])


def mock_response(frame_type, data):
    body_size = 4 + len(data)
    body_size_packed = struct.pack('>l', body_size)
    frame_type_packed = struct.pack('>l', frame_type)
    return body_size_packed + frame_type_packed + data


def mock_response_message(timestamp, attempts, id, body):
    timestamp_packed = struct.pack('>q', timestamp)
    attempts_packed = struct.pack('>h', attempts)
    id = six.b('%016d' % id)
    data = timestamp_packed + attempts_packed + id + body
    return mock_response(nsq.FRAME_TYPE_MESSAGE, data)


def test_connection():
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == b'  V2'
        assert socket.recv(1) == b''

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
        assert socket.recv(4) == b'  V2'
        assert socket.recv(1) == b''

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
    b'hello world',
    b'',
    b'{"some": "json data"}',
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
        assert socket.recv(4) == b'  V2'
        assert socket.recv(9) == b'IDENTIFY\n'

        size = nsq.unpack_size(socket.recv(4))
        data = json.loads(socket.recv(size).decode('utf-8'))

        assert 'gnsq' in data['user_agent']
        socket.send(mock_response(nsq.FRAME_TYPE_RESPONSE, b'OK'))

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()

        assert conn.identify() is None


def test_negotiation():
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == b'  V2'
        assert socket.recv(9) == b'IDENTIFY\n'

        size = nsq.unpack_size(socket.recv(4))
        data = json.loads(socket.recv(size).decode('utf-8'))

        assert 'gnsq' in data['user_agent']
        resp = six.b(json.dumps({'test': 42}))
        socket.send(mock_response(nsq.FRAME_TYPE_RESPONSE, resp))

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()

        assert conn.identify()['test'] == 42


@pytest.mark.parametrize('command,args,resp', [
    ('subscribe', ('topic', 'channel'), b'SUB topic channel\n'),
    ('subscribe', ('foo', 'bar'), b'SUB foo bar\n'),
    ('ready', (0,), b'RDY 0\n'),
    ('ready', (1,), b'RDY 1\n'),
    ('ready', (42,), b'RDY 42\n'),
    ('finish', ('0000000000000000',), b'FIN 0000000000000000\n'),
    ('finish', ('deadbeafdeadbeaf',), b'FIN deadbeafdeadbeaf\n'),
    ('requeue', ('0000000000000000',), b'REQ 0000000000000000 0\n'),
    ('requeue', ('deadbeafdeadbeaf', 0), b'REQ deadbeafdeadbeaf 0\n'),
    ('requeue', ('deadbeafdeadbeaf', 42), b'REQ deadbeafdeadbeaf 42\n'),
    ('touch', ('0000000000000000',), b'TOUCH 0000000000000000\n'),
    ('touch', ('deadbeafdeadbeaf',), b'TOUCH deadbeafdeadbeaf\n'),
    ('close', (), b'CLS\n'),
    ('nop', (), b'NOP\n'),
])
def test_command(command, args, resp):
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == b'  V2'
        assert socket.recv(len(resp)) == resp

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()
        getattr(conn, command)(*args)


def test_publish():
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == b'  V2'
        assert socket.recv(10) == b'PUB topic\n'

        assert nsq.unpack_size(socket.recv(4)) == 3
        assert socket.recv(3) == b'sup'

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()
        conn.publish('topic', b'sup')


def test_multipublish():
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == b'  V2'
        assert socket.recv(11) == b'MPUB topic\n'

        size = nsq.unpack_size(socket.recv(4))
        data = socket.recv(size)

        head, data = data[:4], data[4:]
        assert nsq.unpack_size(head) == 2

        for _ in range(2):
            head, data = data[:4], data[4:]
            assert nsq.unpack_size(head) == 3

            head, data = data[:3], data[3:]
            assert head == b'sup'

        assert data == b''

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()
        conn.multipublish('topic', [b'sup', b'sup'])


def test_deferpublish():
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == b'  V2'
        assert socket.recv(14) == b'DPUB topic 42\n'

        assert nsq.unpack_size(socket.recv(4)) == 3
        assert socket.recv(3) == b'sup'

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()
        conn.publish('topic', b'sup', defer=42)


@pytest.mark.parametrize('error_msg,error,fatal', [
    (b'E_INVALID cannot SUB in current state', 'NSQInvalid', True),
    (b'E_BAD_BODY MPUB failed to read body size', 'NSQBadBody', True),
    (b'E_BAD_TOPIC SUB topic name oh my god is not valid', 'NSQBadTopic', True),
    (b'E_BAD_CHANNEL SUB channel name !! is not valid', 'NSQBadChannel', True),
    (b'E_BAD_MESSAGE PUB failed to read message body', 'NSQBadMessage', True),
    (b'E_PUT_FAILED PUT failed', 'NSQPutFailed', True),
    (b'E_PUB_FAILED PUB failed', 'NSQPubFailed', True),
    (b'E_MPUB_FAILED MPUB failed', 'NSQMPubFailed', True),
    (b'E_AUTH_DISABLED AUTH Disabled', 'NSQAuthDisabled', True),
    (b'E_AUTH_FAILED AUTH failed', 'NSQAuthFailed', True),
    (b'E_UNAUTHORIZED AUTH No authorizations found', 'NSQUnauthorized', True),
    (b'E_FIN_FAILED FIN failed', 'NSQFinishFailed', False),
    (b'E_REQ_FAILED REQ failed', 'NSQRequeueFailed', False),
    (b'E_TOUCH_FAILED TOUCH failed', 'NSQTouchFailed', False),
    (b'some unknown error', 'NSQException', True),
])
def test_error(error_msg, error, fatal):
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == b'  V2'
        socket.send(mock_response(nsq.FRAME_TYPE_ERROR, error_msg))

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()

        frame, resp = conn.read_response()
        assert frame == nsq.FRAME_TYPE_ERROR
        assert isinstance(resp, getattr(errors, error))
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
        assert socket.recv(4) == b'  V2'
        assert socket.recv(9) == b'IDENTIFY\n'

        size = nsq.unpack_size(socket.recv(4))
        data = json.loads(socket.recv(size).decode('utf-8'))

        assert isinstance(data, dict)
        socket.send(mock_response(nsq.FRAME_TYPE_RESPONSE, b'OK'))

        msg = b'SUB topic channel\n'
        assert socket.recv(len(msg)) == msg
        socket.send(mock_response(nsq.FRAME_TYPE_RESPONSE, b'OK'))

        for i in range(10):
            assert socket.recv(6) == b'RDY 1\n'

            body = six.b(json.dumps({'data': {'test_key': i}}))
            ts = i * 1000 * 1000
            socket.send(mock_response_message(ts, i, i, body))

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()

        assert conn.identify() is None

        conn.subscribe('topic', 'channel')
        frame, data = conn.read_response()

        assert frame == nsq.FRAME_TYPE_RESPONSE
        assert data == b'OK'

        for i in range(10):
            conn.ready(1)
            frame, msg = conn.read_response()

            assert frame == nsq.FRAME_TYPE_MESSAGE
            assert isinstance(msg, Message)
            assert msg.timestamp == i * 1000 * 1000
            assert msg.id == six.b('%016d' % i)
            assert msg.attempts == i
            assert json.loads(msg.body.decode('utf-8'))['data']['test_key'] == i


def test_sync_heartbeat():
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == b'  V2'
        socket.send(mock_response(nsq.FRAME_TYPE_RESPONSE, b'_heartbeat_'))
        assert socket.recv(4) == b'NOP\n'

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()

        frame, data = conn.read_response()
        assert frame == nsq.FRAME_TYPE_RESPONSE
        assert data == b'_heartbeat_'


def test_auth():
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == b'  V2'
        assert socket.recv(5) == b'AUTH\n'

        assert nsq.unpack_size(socket.recv(4)) == 6
        assert socket.recv(6) == b'secret'

        resp = six.b(json.dumps({'identity': 'awesome'}))
        socket.send(mock_response(nsq.FRAME_TYPE_RESPONSE, resp))

    with handle as server:
        conn = Nsqd(
            address='127.0.0.1',
            tcp_port=server.server_port,
            auth_secret=b'secret'
        )

        conn.connect()
        resp = conn.auth()
        assert resp['identity'] == 'awesome'


def test_identify_auth():
    @mock_server
    def handle(socket, address):
        assert socket.recv(4) == b'  V2'
        assert socket.recv(9) == b'IDENTIFY\n'

        size = nsq.unpack_size(socket.recv(4))
        data = json.loads(socket.recv(size).decode('utf-8'))
        assert 'gnsq' in data['user_agent']

        resp = six.b(json.dumps({'auth_required': True}))
        socket.send(mock_response(nsq.FRAME_TYPE_RESPONSE, resp))

        assert socket.recv(5) == b'AUTH\n'
        assert nsq.unpack_size(socket.recv(4)) == 6
        assert socket.recv(6) == b'secret'

        resp = six.b(json.dumps({'identity': 'awesome'}))
        socket.send(mock_response(nsq.FRAME_TYPE_RESPONSE, resp))

    with handle as server:
        conn = Nsqd(
            address='127.0.0.1',
            tcp_port=server.server_port,
            auth_secret=b'secret'
        )

        @conn.on_auth.connect
        def assert_auth(conn, response):
            assert assert_auth.was_called is False
            assert_auth.was_called = True
            assert response['identity'] == 'awesome'

        assert_auth.was_called = False
        conn.connect()
        resp = conn.identify()

        assert resp['auth_required']
        assert assert_auth.was_called


@pytest.mark.parametrize('tls,deflate,snappy', product((True, False), repeat=3))
@pytest.mark.slow
@pytest.mark.timeout(60)
def test_socket_upgrades(tls, deflate, snappy):
    with NsqdIntegrationServer() as server:
        options = {
            'address': server.address,
            'tcp_port': server.tcp_port,
            'deflate': deflate,
            'snappy': snappy,
        }

        if tls:
            options.update({
                'tls_v1': True,
                'tls_options': {
                    'keyfile': server.tls_key,
                    'certfile': server.tls_cert,
                }
            })

        conn = Nsqd(**options)
        conn.connect()
        assert conn.state == states.CONNECTED

        if deflate and snappy:
            with pytest.raises(errors.NSQErrorCode):
                conn.identify()
            return

        if tls and BAD_GEVENT:
            with pytest.raises(AttributeError):
                conn.identify()
            return

        if tls and server.version < (0, 2, 28):
            with pytest.raises(ssl.SSLError):
                conn.identify()
            return

        resp = conn.identify()
        assert isinstance(resp, dict)

        assert resp['tls_v1'] is tls
        assert resp['deflate'] is deflate
        assert resp['snappy'] is snappy

        if tls and (deflate or snappy):
            assert isinstance(conn.stream.socket._socket, SSLSocket)
        elif tls:
            assert isinstance(conn.stream.socket, SSLSocket)

        if deflate:
            assert isinstance(conn.stream.socket, DefalteSocket)

        if snappy:
            assert isinstance(conn.stream.socket, SnappySocket)

        conn.publish('topic', b'sup')
        frame, data = conn.read_response()
        assert frame == nsq.FRAME_TYPE_RESPONSE
        assert data == b'OK'

        conn.subscribe('topic', 'channel')
        frame, data = conn.read_response()
        assert frame == nsq.FRAME_TYPE_RESPONSE
        assert data == b'OK'

        conn.ready(1)
        frame, data = conn.read_response()
        assert frame == nsq.FRAME_TYPE_MESSAGE
        assert data.body == b'sup'

        conn.close_stream()


@pytest.mark.slow
@pytest.mark.timeout(60)
def test_cls_error():
    with NsqdIntegrationServer() as server:
        conn = Nsqd(address=server.address, tcp_port=server.tcp_port)

        conn.connect()
        assert conn.state == states.CONNECTED

        conn.close()
        frame, error = conn.read_response()
        assert frame == nsq.FRAME_TYPE_ERROR
        assert isinstance(error, errors.NSQInvalid)
