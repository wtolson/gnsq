# -*- coding: utf-8 -*-
from __future__ import absolute_import

from mmap import PAGESIZE
from errno import ENOTCONN, EDEADLK, EAGAIN, EWOULDBLOCK

import gevent
from gevent import socket
from gevent.queue import Queue
from gevent.event import AsyncResult

try:
    from gevent.ssl import SSLSocket, PROTOCOL_TLSv1, CERT_NONE
except ImportError:
    SSLSocket = None  # pyflakes.ignore

from gnsq.states import INIT, CONNECTED, DISCONNECTED
from gnsq.errors import NSQSocketError

try:
    from .snappy import SnappySocket
except ImportError:
    SnappySocket = None  # pyflakes.ignore

from .defalte import DefalteSocket


class Stream(object):
    def __init__(self, address, port, timeout, buffer_size=PAGESIZE):
        self.address = address
        self.port = port
        self.timeout = timeout

        self.buffer = b''
        self.buffer_size = buffer_size

        self.socket = None
        self.worker = None
        self.queue = Queue()
        self.state = INIT

    @property
    def is_connected(self):
        return self.state == CONNECTED

    def ensure_connection(self):
        if self.is_connected:
            return
        raise NSQSocketError(ENOTCONN, 'Socket is not connected')

    def connect(self):
        if self.state not in (INIT, DISCONNECTED):
            return

        try:
            self.socket = socket.create_connection(
                address=(self.address, self.port),
                timeout=self.timeout,
            )

        except socket.error as error:
            raise NSQSocketError(*error.args)

        self.state = CONNECTED
        self.worker = gevent.spawn(self.send_loop)

    def read(self, size):
        while len(self.buffer) < size:
            self.ensure_connection()

            try:
                packet = self.socket.recv(self.buffer_size)
            except socket.error as error:
                if error.errno in (EDEADLK, EAGAIN, EWOULDBLOCK):
                    gevent.sleep()
                    continue
                raise NSQSocketError(*error.args)

            if not packet:
                self.close()

            self.buffer += packet

        data = self.buffer[:size]
        self.buffer = self.buffer[size:]

        return data

    def send(self, data, async=False):
        self.ensure_connection()

        result = AsyncResult()
        self.queue.put((data, result))

        if async:
            return result

        result.get()

    def consume_buffer(self):
        data = self.buffer
        self.buffer = b''
        return data

    def close(self):
        if not self.is_connected:
            return

        self.state = DISCONNECTED
        self.queue.put(StopIteration)

    def send_loop(self):
        for data, result in self.queue:
            if not self.is_connected:
                error = NSQSocketError(ENOTCONN, 'Socket is not connected')
                result.set_exception(error)

            try:
                self.socket.sendall(data)
                result.set()

            except socket.error as error:
                result.set_exception(NSQSocketError(*error.args))

            except Exception as error:
                result.set_exception(error)

        self.socket.close()

    def upgrade_to_tls(
        self,
        keyfile=None,
        certfile=None,
        cert_reqs=CERT_NONE,
        ca_certs=None
    ):
        if SSLSocket is None:
            msg = 'tls_v1 requires Python 2.6+ or Python 2.5 w/ pip install ssl'
            raise RuntimeError(msg)

        self.ensure_connection()
        self.socket = SSLSocket(
            self.socket,
            keyfile=keyfile,
            certfile=certfile,
            cert_reqs=cert_reqs,
            ca_certs=ca_certs,
            ssl_version=PROTOCOL_TLSv1,
        )

    def upgrade_to_snappy(self):
        if SnappySocket is None:
            raise RuntimeError('snappy requires the python-snappy package')

        self.ensure_connection()
        self.socket = SnappySocket(self.socket)
        self.socket.bootstrap(self.consume_buffer())

    def upgrade_to_defalte(self, level):
        self.ensure_connection()
        self.socket = DefalteSocket(self.socket, level)
        self.socket.bootstrap(self.consume_buffer())
