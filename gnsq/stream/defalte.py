# -*- coding: utf-8 -*-
from __future__ import absolute_import
import zlib
from .compression import CompressionSocket


class DefalteSocket(CompressionSocket):
    def __init__(self, socket, level):
        wbits = -zlib.MAX_WBITS
        self._decompressor = zlib.decompressobj(wbits)
        self._compressor = zlib.compressobj(level, zlib.DEFLATED, wbits)
        super(DefalteSocket, self).__init__(socket)

    def compress(self, data):
        data = self._compressor.compress(data)
        return data + self._compressor.flush(zlib.Z_SYNC_FLUSH)

    def decompress(self, data):
        return self._decompressor.decompress(data)

    def close(self):
        self._socket.sendall(self._compressor.flush(zlib.Z_FINISH))
        self._socket.close()
