# -*- coding: utf-8 -*-
from __future__ import absolute_import

from mmap import PAGESIZE
from errno import ENOTCONN, EDEADLK, EAGAIN, EWOULDBLOCK

import six

import gevent
from gevent import socket
from gevent.lock import Semaphore
from gevent.ssl import SSLSocket, PROTOCOL_TLSv1_2, CERT_NONE

from gnsq.errors import NSQSocketError

try:
    from .snappy import SnappySocket
except ImportError:
    SnappySocket = None  # pyflakes.ignore

from .defalte import DefalteSocket


class Stream(object):
    def __init__(self, address, port, timeout, buffer_size=PAGESIZE,
                 lock_class=Semaphore):
        self.address = address
        self.port = port
        self.timeout = timeout

        self.buffer = b''
        self.buffer_size = buffer_size

        self.socket = None
        self.lock = lock_class()

    @property
    def is_connected(self):
        return self.socket is not None

    def ensure_connection(self):
        if self.is_connected:
            return
        raise NSQSocketError(ENOTCONN, 'Socket is not connected')

    def connect(self):
        if self.is_connected:
            return

        try:
            self.socket = socket.create_connection(
                address=(self.address, self.port),
                timeout=self.timeout,
            )

        except socket.error as error:
            six.raise_from(NSQSocketError(*error.args), error)

    def read(self, size):
        while len(self.buffer) < size:
            self.ensure_connection()

            try:
                packet = self.socket.recv(self.buffer_size)
            except socket.error as error:
                if error.errno in (EDEADLK, EAGAIN, EWOULDBLOCK):
                    gevent.sleep()
                    continue
                six.raise_from(NSQSocketError(*error.args), error)

            if not packet:
                self.close()

            self.buffer += packet

        data = self.buffer[:size]
        self.buffer = self.buffer[size:]

        return data

    def send(self, data):
        self.ensure_connection()

        with self.lock:
            try:
                return self.socket.sendall(data)
            except socket.error as error:
                six.raise_from(NSQSocketError(*error.args), error)

    def consume_buffer(self):
        data = self.buffer
        self.buffer = b''
        return data

    def close(self):
        if not self.is_connected:
            return

        socket = self.socket
        self.socket = None
        self.buffer = b''

        socket.close()

    def upgrade_to_tls(
        self,
        keyfile=None,
        certfile=None,
        cert_reqs=CERT_NONE,
        ca_certs=None,
        ssl_version=PROTOCOL_TLSv1_2
    ):
        self.ensure_connection()

        try:
            self.socket = SSLSocket(
                self.socket,
                keyfile=keyfile,
                certfile=certfile,
                cert_reqs=cert_reqs,
                ca_certs=ca_certs,
                ssl_version=ssl_version,
            )
        except socket.error as error:
            six.raise_from(NSQSocketError(*error.args), error)

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
