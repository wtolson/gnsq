from __future__ import absolute_import

import zlib
from .compression import CompressionSocket


class DefalteSocket(CompressionSocket):
    def __init__(self, socket, level):
        self._decompressor = zlib.decompressobj(level)
        self._compressor = zlib.compressobj(level)
        super(DefalteSocket, self).__init__(socket)

    def compress(self, data):
        return self._compressor.compress(data)

    def decompress(self, data):
        return self._decompressor.decompress(data)
