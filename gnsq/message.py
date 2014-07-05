# -*- coding: utf-8 -*-
from __future__ import absolute_import
from .errors import NSQException


class Message(object):
    """A class representing a message received from nsqd."""

    def __init__(self, conn, timestamp, attempts, id, body):
        self._has_responded = False
        self.conn = conn
        self.timestamp = timestamp
        self.attempts = attempts
        self.id = id
        self.body = body

    def has_responded(self):
        """Returns whether or not this message has been responded to."""
        return self._has_responded

    def finish(self):
        """
        Respond to nsqd that you’ve processed this message successfully
        (or would like to silently discard it).
        """
        if self._has_responded:
            raise NSQException('already responded')
        self._has_responded = True
        self.conn.finish(self.id)

    def requeue(self, time_ms=0):
        """
        Respond to nsqd that you’ve failed to process this message successfully
        (and would like it to be requeued).
        """
        if self._has_responded:
            raise NSQException('already responded')
        self._has_responded = True
        self.conn.requeue(self.id, time_ms)

    def touch(self):
        """Respond to nsqd that you need more time to process the message."""
        if self._has_responded:
            raise NSQException('already responded')
        self.conn.touch(self.id)
