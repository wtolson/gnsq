# -*- coding: utf-8 -*-
from __future__ import absolute_import, division

import random
from multiprocessing import cpu_count

from gevent.queue import Queue

from .consumer import Consumer
from .decorators import deprecated
from .errors import NSQNoConnections
from .states import INIT


class Reader(Consumer):
    """Use :class:`~gnsq.Consumer` instead.

    .. deprecated:: 1.0.0
    """
    @deprecated
    def __init__(self, *args, **kwargs):
        """Use :class:`~gnsq.Consumer` instead.

        .. deprecated:: 1.0.0
        """
        setattr(self, 'async', kwargs.pop('async', False))

        max_concurrency = kwargs.pop('max_concurrency', 0)

        if max_concurrency < 0:
            self.max_concurrency = cpu_count()
        else:
            self.max_concurrency = max_concurrency

        if self.max_concurrency:
            self.queue = Queue()
        else:
            self.queue = None

        super(Reader, self).__init__(*args, **kwargs)

    def start(self, *args, **kwargs):
        if self._state == INIT:
            for _ in range(self.max_concurrency):
                self._killables.add(self._workers.spawn(self._run))

        return super(Reader, self).start(*args, **kwargs)

    def handle_message(self, conn, message):
        if self.max_concurrency:
            self.logger.debug('[%s] queueing message: %s' % (conn, message.id))
            self.queue.put((conn, message))
        else:
            super(Reader, self).handle_message(conn, message)

    @deprecated
    def publish(self, topic, message):
        """Use :class:`~gnsq.Producer` instead.

        .. deprecated:: 1.0.0
        """
        if not self.connections:
            raise NSQNoConnections()
        conn = random.choice(list(self.connections))
        conn.publish(topic, message)

    def _handle_message(self, message):
        if getattr(self, 'async'):
            message.enable_async()
        return super(Reader, self)._handle_message(message)

    def _run(self):
        for conn, message in self.queue:
            if not conn.is_connected:
                continue
            super(Reader, self).handle_message(conn, message)
