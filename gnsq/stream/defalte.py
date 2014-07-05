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
        return self._compressor.compress(data)

    def decompress(self, data):
        return self._decompressor.decompress(data)
