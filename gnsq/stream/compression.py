# -*- coding: utf-8 -*-
from __future__ import absolute_import
from errno import EWOULDBLOCK
from gnsq.errors import NSQSocketError


class CompressionSocket(object):
    def __init__(self, socket):
        self._socket = socket
        self._bootstrapped = None

    def __getattr__(self, name):
        return getattr(self._socket, name)

    def bootstrap(self, data):
        if not data:
            return
        self._bootstrapped = self.decompress(data)

    def recv(self, size):
        if self._bootstrapped:
            data = self._bootstrapped
            self._bootstrapped = None
            return data

        chunk = self._socket.recv(size)
        if chunk:
            uncompressed = self.decompress(chunk)

        if not uncompressed:
            raise NSQSocketError(EWOULDBLOCK, 'Operation would block')

        return uncompressed

    def send(self, data):
        self._socket.send(self.compress(data))
