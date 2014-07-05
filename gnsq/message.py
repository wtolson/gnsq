# -*- coding: utf-8 -*-
from __future__ import absolute_import
from .errors import NSQException


class Message(object):
    def __init__(self, conn, timestamp, attempts, id, body):
        self._has_responded = False
        self.conn = conn
        self.timestamp = timestamp
        self.attempts = attempts
        self.id = id
        self.body = body

    def has_responded(self):
        return self._has_responded

    def finish(self):
        if self._has_responded:
            raise NSQException('already responded')
        self._has_responded = True
        self.conn.finish(self.id)

    def requeue(self, time_ms=0):
        if self._has_responded:
            raise NSQException('already responded')
        self._has_responded = True
        self.conn.requeue(self.id, time_ms)

    def touch(self):
        if self._has_responded:
            raise NSQException('already responded')
        self.conn.touch(self.id)
