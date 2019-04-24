# -*- coding: utf-8 -*-
import logging
import warnings

import gevent.queue
import gevent.pool

from gnsq.errors import NSQException


TIMEOUT_WARNING = 'batching timed out. batch size may be to large'
STARVED_WARNING = 'consumer is starved. batch size may be to large'


class BatchHandler(object):
    """Batch message handler for gnsq.

    It is recommended to use a max inflight greater than the batch size.

    Example usage::

        >>> consumer = Consumer('topic', 'worker', max_in_flight=16)
        >>> consumer.on_message.connect(BatchHandler(8, my_handler), weak=False)
    """
    def __init__(self, batch_size, handle_batch=None, handle_message=None,
                 handle_batch_error=None, handle_message_error=None,
                 timeout=10, spawn=gevent.spawn):
        self.logger = logging.getLogger(__name__)
        self.message_channel = gevent.queue.Channel()
        self.batch_size = batch_size
        self.timeout = timeout

        if isinstance(spawn, int):
            spawn = gevent.pool.Pool(spawn).spawn

        self.spawn = spawn

        if handle_batch is not None:
            self.handle_batch = handle_batch

        if handle_message is not None:
            self.handle_message = handle_message

        if handle_batch_error is not None:
            self.handle_batch_error = handle_batch_error

        if handle_message_error is not None:
            self.handle_message_error = handle_message_error

        self.worker = gevent.spawn(self._run)

    def __call__(self, consumer, message):
        message.enable_async()
        self.message_channel.put(message)

        if consumer.is_starved:
            self.message_channel.put(StopIteration)

    def _run(self):
        while True:
            messages = []

            while len(messages) < self.batch_size:
                try:
                    message = self.message_channel.get(timeout=self.timeout)
                except gevent.queue.Empty:
                    warnings.warn(TIMEOUT_WARNING, RuntimeWarning)
                    break

                if message is StopIteration:
                    warnings.warn(STARVED_WARNING, RuntimeWarning)
                    break

                messages.append(message)

            if messages:
                self.spawn(self.run_batch, messages)

    def finish_message(self, message):
        try:
            message.finish()
        except NSQException as error:
            self.logger.warning('error finishing message (%r)', error)

    def finish_messages(self, messages):
        for message in messages:
            if message.has_responded():
                continue
            self.finish_message(message)

    def requeue_message(self, message):
        try:
            message.requeue()
        except NSQException as error:
            self.logger.warning('error requeueing message (%r)', error)

    def requeue_messages(self, messages):
        for message in messages:
            if message.has_responded():
                continue
            self.requeue_message(message)

    def run_batch(self, messages):
        batch = []

        for message in messages:
            try:
                batch.append(self.handle_message(message))
            except Exception as error:
                self.logger.exception('caught exception while handling message')
                self.handle_message_error(error, message)
                self.requeue_message(message)

        if batch:
            try:
                self.handle_batch(batch)
            except Exception as error:
                self.logger.exception('caught exception while handling batch')
                self.handle_batch_error(error, messages, batch)
                self.requeue_messages(messages)
                return

        self.finish_messages(messages)

    def handle_message(self, message):
        """Handle a single message.

        Over ride this to provide some processing and an individual message.
        The result of this function is what is passed to :meth:`handle_batch`.
        This may be overridden or passed into the constructor. By default it
        simply returns the message.

        Raising an exception in :meth:`handle_message` will cause that message
        to be requeued and excluded from the batch.
        """
        return message

    def handle_batch(self, messages):
        """Handle a batch message.

        Processes a batch of messages. You must provide a :meth:`handle_batch`
        function to the constructor or override this method.

        Raising an exception in :meth:`handle_batch` will cause all messages in
        the batch to be requeued.
        """
        raise RuntimeError('handle_message must be overridden')

    def handle_message_error(self, error, message):
        """Handle an exception processesing an individual message.

        This may be overridden or passed into the constructor.
        """
        pass

    def handle_batch_error(self, error, messages, batch):
        """Handle an exception processsing a batch of messages.

        This may be overridden or passed into the constructor.
        """
        pass
