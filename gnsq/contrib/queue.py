# -*- coding: utf-8 -*-
import gevent.queue


class QueueHandler(gevent.queue.Queue):
    """Iterator like api for gnsq.

    Example usage::

        >>> queue = QueueHandler()
        >>> consumer = Consumer('topic', 'worker', max_in_flight=16)
        >>> consumer.on_message.connect(queue)
        >>> consumer.start(block=False)
        >>> for message in queue:
        ...     print(message.body)
        ...     message.finish()

    Or give it to a pool::

        >>> gevent.pool.Pool().map(queue, my_handler)

    :param maxsize: maximum number of messages that can be queued. If less than
        or equal to zero or None, the queue size is infinite.
    """

    def __call__(self, consumer, message):
        message.enable_async()
        self.put(message)


class ChannelHandler(gevent.queue.Channel):
    """Iterator like api for gnsq.

    Like :class:`QueueHandler` with a ``maxsize`` of ``1``.
    """

    def __call__(self, consumer, message):
        message.enable_async()
        self.put(message)
