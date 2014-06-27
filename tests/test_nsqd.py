from __future__ import with_statement
import struct

from gevent.event import AsyncResult
from gevent.server import StreamServer

from gnsq import Nsqd, states


class nsqd_handler(object):
    def __init__(self, handler):
        self.handler = handler
        self.result = AsyncResult()
        self.server = StreamServer(('127.0.0.1', 0), self)

    def __call__(self, socket, address):
        try:
            self.handler(socket, address)
            self.result.set()
        except Exception as error:
            self.result.set_exception(error)

    def __enter__(self):
        self.server.start()
        return self.server

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.result.get()
        self.server.stop()


def test_connection():

    @nsqd_handler
    def handle(socket, address):
        assert socket.recv(4) == '  V2'

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        assert conn.state == states.INIT

        conn.connect()
        assert conn.state == states.CONNECTED

        conn.close_stream()
        assert conn.state == states.DISCONNECTED


def test_read():
    body = 'hello world'

    @nsqd_handler
    def handle(socket, address):
        socket.send(struct.pack('>l', len(body)))
        socket.send(body)

    with handle as server:
        conn = Nsqd(address='127.0.0.1', tcp_port=server.server_port)
        conn.connect()

        assert conn._read_response() == body
        conn.close_stream()
