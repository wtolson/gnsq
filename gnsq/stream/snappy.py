# -*- coding: utf-8 -*-
from __future__ import absolute_import
import snappy
from .compression import CompressionSocket


class SnappySocket(CompressionSocket):
    def __init__(self, socket):
        self._decompressor = snappy.StreamDecompressor()
        self._compressor = snappy.StreamCompressor()
        super(SnappySocket, self).__init__(socket)

    def compress(self, data):
        return self._compressor.add_chunk(data, compress=True)

    def decompress(self, data):
        return self._decompressor.decompress(data)
