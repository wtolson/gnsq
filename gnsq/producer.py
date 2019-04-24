# -*- coding: utf-8 -*-
from __future__ import absolute_import, division

import logging
from collections import defaultdict, deque

import blinker
import gevent

from gevent.event import AsyncResult
from gevent.pool import Group
from gevent.queue import Queue, Empty

from . import protocol as nsq

from .backofftimer import BackoffTimer
from .decorators import cached_property
from .errors import NSQException, NSQNoConnections
from .nsqd import NsqdTCPClient
from .states import INIT, RUNNING, CLOSED
from .util import parse_nsqds


class Producer(object):
    """High level NSQ producer.

    A Producer will connect to the nsqd tcp addresses and support async
    publishing (``PUB`` & ``MPUB`` & ``DPUB``) of messages to `nsqd` over the
    TCP protocol.

    Example publishing a message::

        from gnsq import Producer

        producer = Producer('localhost:4150')
        producer.start()
        producer.publish('topic', b'hello world')

    :param nsqd_tcp_addresses: a sequence of string addresses of the nsqd
        instances this consumer should connect to

    :param max_backoff_duration: the maximum time we will allow a backoff state
        to last in seconds. If zero, backoff wil not occur

    :param **kwargs: passed to :class:`~gnsq.NsqdTCPClient` initialization
    """
    def __init__(self, nsqd_tcp_addresses=[], max_backoff_duration=128,
                 **kwargs):
        if not nsqd_tcp_addresses:
            raise ValueError('must specify at least one nsqd or lookupd')

        self.nsqd_tcp_addresses = parse_nsqds(nsqd_tcp_addresses)
        self.max_backoff_duration = max_backoff_duration
        self.conn_kwargs = kwargs
        self.logger = logging.getLogger(__name__)

        self._state = INIT
        self._connections = Queue()
        self._connection_backoffs = defaultdict(self._create_backoff)
        self._response_queues = {}
        self._workers = Group()

    @cached_property
    def on_response(self):
        """Emitted when a response is received.

        The signal sender is the consumer and the ` ` is sent as an
        argument.
        """
        return blinker.Signal(doc='Emitted when a response is received.')

    @cached_property
    def on_error(self):
        """Emitted when an error is received.

        The signal sender is the consumer and the ``error`` is sent as an
        argument.
        """
        return blinker.Signal(doc='Emitted when a error is received.')

    @cached_property
    def on_auth(self):
        """Emitted after a connection is successfully authenticated.

        The signal sender is the consumer and the ``conn`` and parsed
        ``response`` are sent as arguments.
        """
        return blinker.Signal(doc='Emitted when a response is received.')

    @cached_property
    def on_close(self):
        """Emitted after :meth:`close`.

        The signal sender is the consumer.
        """
        return blinker.Signal(doc='Emitted after the consumer is closed.')

    def start(self):
        """Start discovering and listing to connections."""
        if self._state == CLOSED:
            raise NSQException('producer already closed')

        if self.is_running:
            self.logger.warn('producer already started')
            return

        self.logger.debug('starting producer...')
        self._state = RUNNING

        for address in self.nsqd_tcp_addresses:
            address, port = address.split(':')
            self.connect_to_nsqd(address, int(port))

    def close(self):
        """Immediately close all connections and stop workers."""
        if not self.is_running:
            return

        self._state = CLOSED
        self.logger.debug('closing connection(s)')

        while True:
            try:
                conn = self._connections.get(block=False)
            except Empty:
                break

            conn.close_stream()

        self.on_close.send(self)

    def join(self, timeout=None, raise_error=False):
        """Block until all connections have closed and workers stopped."""
        self._workers.join(timeout, raise_error)

    @property
    def is_running(self):
        """Check if the producer is currently running."""
        return self._state == RUNNING

    def connect_to_nsqd(self, address, port):
        if not self.is_running:
            return

        conn = NsqdTCPClient(address, port, **self.conn_kwargs)
        self.logger.debug('[%s] connecting...', conn)

        conn.on_response.connect(self.handle_response)
        conn.on_error.connect(self.handle_error)
        conn.on_auth.connect(self.handle_auth)

        try:
            conn.connect()
            conn.identify()

        except NSQException as error:
            self.logger.warn('[%s] connection failed (%r)', conn, error)
            self.handle_connection_failure(conn)
            return

        # Check if we've closed since we started
        if not self.is_running:
            self.handle_connection_failure(conn)
            return

        self.logger.info('[%s] connection successful', conn)
        self.handle_connection_success(conn)

    def _listen(self, conn):
        try:
            conn.listen()
        except NSQException as error:
            self.logger.warning('[%s] connection lost (%r)', conn, error)

        self.handle_connection_failure(conn)

    def handle_connection_success(self, conn):
        self._response_queues[conn] = deque()
        self._put_connection(conn)
        self._workers.spawn(self._listen, conn)
        self._connection_backoffs[conn].success()

    def handle_connection_failure(self, conn):
        conn.close_stream()
        self._clear_responses(conn, NSQException('connection closed'))

        if not self.is_running:
            return

        seconds = self._connection_backoffs[conn].failure().get_interval()
        self.logger.debug('[%s] retrying in %ss', conn, seconds)

        gevent.spawn_later(
            seconds, self.connect_to_nsqd, conn.address, conn.port)

    def handle_auth(self, conn, response):
        metadata = []
        if response.get('identity'):
            metadata.append("Identity: %r" % response['identity'])

        if response.get('permission_count'):
            metadata.append("Permissions: %d" % response['permission_count'])

        if response.get('identity_url'):
            metadata.append(response['identity_url'])

        self.logger.info('[%s] AUTH accepted %s', conn, ' '.join(metadata))
        self.on_auth.send(self, conn=conn, response=response)

    def handle_response(self, conn, response):
        self.logger.debug('[%s] response: %s', conn, response)

        if response == nsq.OK:
            result = self._response_queues[conn].popleft()
            result.set(response)

        self.on_response.send(self, response=response)

    def handle_error(self, conn, error):
        self.logger.debug('[%s] error: %s', conn, error)
        self._clear_responses(conn, error)
        self.on_error.send(self, error=error)

    def _create_backoff(self):
        return BackoffTimer(max_interval=self.max_backoff_duration)

    def _clear_responses(self, conn, error):
        # All relevent errors are fatal
        for result in self._response_queues.pop(conn, []):
            result.set_exception(error)

    def _get_connection(self, block=True, timeout=None):
        if not self.is_running:
            raise NSQException('producer not running')

        while True:
            try:
                conn = self._connections.get(block=block, timeout=timeout)
            except Empty:
                raise NSQNoConnections

            if conn.is_connected:
                return conn

            # Discard closed connections

    def _put_connection(self, conn):
        if not self.is_running:
            return
        self._connections.put(conn)

    def publish(self, topic, data, defer=None, block=True, timeout=None,
                raise_error=True):
        """Publish a message to the given topic.

        :param topic: the topic to publish to

        :param data: bytestring data to publish

        :param defer: duration in milliseconds to defer before publishing
            (requires nsq 0.3.6)

        :param block: wait for a connection to become available before
            publishing the message. If block is `False` and no connections
            are available, :class:`~gnsq.errors.NSQNoConnections` is raised

        :param timeout: if timeout is a positive number, it blocks at most
            ``timeout`` seconds before raising
            :class:`~gnsq.errors.NSQNoConnections`

        :param raise_error: if ``True``, it blocks until a response is received
            from the nsqd server, and any error response is raised. Otherwise
            an :class:`~gevent.event.AsyncResult` is returned
        """
        result = AsyncResult()
        conn = self._get_connection(block=block, timeout=timeout)

        try:
            self._response_queues[conn].append(result)
            conn.publish(topic, data, defer=defer)
        finally:
            self._put_connection(conn)

        if raise_error:
            return result.get()

        return result

    def multipublish(self, topic, messages, block=True, timeout=None,
                     raise_error=True):
        """Publish an iterable of messages to the given topic.

        :param topic: the topic to publish to

        :param messages: iterable of bytestrings to publish

        :param block: wait for a connection to become available before
            publishing the message. If block is `False` and no connections
            are available, :class:`~gnsq.errors.NSQNoConnections` is raised

        :param timeout: if timeout is a positive number, it blocks at most
            ``timeout`` seconds before raising
            :class:`~gnsq.errors.NSQNoConnections`

        :param raise_error: if ``True``, it blocks until a response is received
            from the nsqd server, and any error response is raised. Otherwise
            an :class:`~gevent.event.AsyncResult` is returned
        """
        result = AsyncResult()
        conn = self._get_connection(block=block, timeout=timeout)

        try:
            self._response_queues[conn].append(result)
            conn.multipublish(topic, messages)
        finally:
            self._put_connection(conn)

        if raise_error:
            return result.get()

        return result
