# -*- coding: utf-8 -*-
from __future__ import absolute_import, division

import logging
import random
import time

from collections import defaultdict
from itertools import cycle

import blinker
import gevent

from gevent.event import Event
from gevent.pool import Group

from .backofftimer import BackoffTimer
from .decorators import cached_property
from .errors import NSQException, NSQRequeueMessage, NSQSocketError
from .nsqd import NsqdTCPClient
from .states import INIT, RUNNING, BACKOFF, THROTTLED, CLOSED
from .util import parse_nsqds, parse_lookupds


class Consumer(object):
    """High level NSQ consumer.

    A Consumer will connect to the nsqd tcp addresses or poll the provided
    nsqlookupd http addresses for the configured topic and send signals to
    message handlers connected to the :attr:`on_message` signal or provided by
    ``message_handler``.

    Messages will automatically be finished when the message handle returns
    unless :meth:`message.enable_async() <gnsq.Message.enable_async>` is called.
    If an exception occurs or :class:`~gnsq.errors.NSQRequeueMessage` is raised,
    the message will be requeued.

    The Consumer will handle backing off of failed messages up to a configurable
    ``max_interval`` as well as automatically reconnecting to dropped
    connections.

    Example usage::

        from gnsq import Consumer

        consumer = gnsq.Consumer('topic', 'channel', 'localhost:4150')

        @consumer.on_message.connect
        def handler(consumer, message):
            print 'got message:', message.body

        consumer.start()

    :param topic: specifies the desired NSQ topic

    :param channel: specifies the desired NSQ channel

    :param nsqd_tcp_addresses: a sequence of string addresses of the nsqd
        instances this consumer should connect to

    :param lookupd_http_addresses: a sequence of string addresses of the
        nsqlookupd instances this consumer should query for producers of the
        specified topic

    :param name: a string that is used for logging messages (defaults to
        ``'gnsq.consumer.{topic}.{channel}'``)

    :param message_handler: the callable that will be executed for each message
        received

    :param max_tries: the maximum number of attempts the consumer will make to
        process a message after which messages will be automatically discarded

    :param max_in_flight: the maximum number of messages this consumer will
        pipeline for processing. this value will be divided evenly amongst the
        configured/discovered nsqd producers

    :param requeue_delay: the default delay to use when requeueing a failed
        message

    :param lookupd_poll_interval: the amount of time in seconds between querying
        all of the supplied nsqlookupd instances.  A random amount of time based
        on this value will be initially introduced in order to add jitter when
        multiple consumers are running

    :param lookupd_poll_jitter: the maximum fractional amount of jitter to add
        to the lookupd poll loop. This helps evenly distribute requests even if
        multiple consumers restart at the same time.

    :param low_ready_idle_timeout: the amount of time in seconds to wait for a
        message from a producer when in a state where RDY counts are
        re-distributed (ie. `max_in_flight` < `num_producers`)

    :param max_backoff_duration: the maximum time we will allow a backoff state
        to last in seconds. If zero, backoff wil not occur

    :param backoff_on_requeue: if ``False``, backoff will only occur on
        exception

    :param **kwargs: passed to :class:`~gnsq.NsqdTCPClient` initialization
    """
    def __init__(self, topic, channel, nsqd_tcp_addresses=[],
                 lookupd_http_addresses=[], name=None, message_handler=None,
                 max_tries=5, max_in_flight=1, requeue_delay=0,
                 lookupd_poll_interval=60, lookupd_poll_jitter=0.3,
                 low_ready_idle_timeout=10, max_backoff_duration=128,
                 backoff_on_requeue=True, **kwargs):
        if not nsqd_tcp_addresses and not lookupd_http_addresses:
            raise ValueError('must specify at least one nsqd or lookupd')

        self.nsqd_tcp_addresses = parse_nsqds(nsqd_tcp_addresses)
        self.lookupds = parse_lookupds(lookupd_http_addresses)
        self.iterlookupds = cycle(self.lookupds)

        self.topic = topic
        self.channel = channel
        self.max_tries = max_tries
        self.max_in_flight = max_in_flight
        self.requeue_delay = requeue_delay
        self.lookupd_poll_interval = lookupd_poll_interval
        self.lookupd_poll_jitter = lookupd_poll_jitter
        self.low_ready_idle_timeout = low_ready_idle_timeout
        self.backoff_on_requeue = backoff_on_requeue
        self.max_backoff_duration = max_backoff_duration
        self.conn_kwargs = kwargs

        if name:
            self.name = name
        else:
            self.name = '%s.%s.%s' % (__name__, self.topic, self.channel)

        if message_handler is not None:
            self.on_message.connect(message_handler, weak=False)

        self.logger = logging.getLogger(self.name)

        self._state = INIT
        self._redistributed_ready_event = Event()
        self._connection_backoffs = defaultdict(self._create_backoff)
        self._message_backoffs = defaultdict(self._create_backoff)

        self._connections = {}
        self._workers = Group()
        self._killables = Group()

    @cached_property
    def on_message(self):
        """Emitted when a message is received.

        The signal sender is the consumer and the ``message`` is sent as an
        argument. The ``message_handler`` param is connected to this signal.
        """
        return blinker.Signal(doc='Emitted when a message is received.')

    @cached_property
    def on_response(self):
        """Emitted when a response is received.

        The signal sender is the consumer and the ``response`` is sent as an
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
    def on_finish(self):
        """Emitted after a message is successfully finished.

        The signal sender is the consumer and the ``message_id`` is sent as an
        argument.
        """
        return blinker.Signal(doc='Emitted after the a message is finished.')

    @cached_property
    def on_requeue(self):
        """Emitted after a message is requeued.

        The signal sender is the consumer and the ``message_id`` and ``timeout``
        are sent as arguments.
        """
        return blinker.Signal(doc='Emitted after the a message is requeued.')

    @cached_property
    def on_giving_up(self):
        """Emitted after a giving up on a message.

        Emitted when a message has exceeded the maximum number of attempts
        (``max_tries``) and will no longer be requeued. This is useful to
        perform tasks such as writing to disk, collecting statistics etc. The
        signal sender is the consumer and the ``message`` is sent as an
        argument.
        """
        return blinker.Signal(doc='Sent after a giving up on a message.')

    @cached_property
    def on_auth(self):
        """Emitted after a connection is successfully authenticated.

        The signal sender is the consumer and the ``conn`` and parsed
        ``response`` are sent as arguments.
        """
        return blinker.Signal(doc='Emitted when a response is received.')

    @cached_property
    def on_exception(self):
        """Emitted when an exception is caught while handling a message.

        The signal sender is the consumer and the ``message`` and ``error`` are
        sent as arguments.
        """
        return blinker.Signal(doc='Emitted when an exception is caught.')

    @cached_property
    def on_close(self):
        """Emitted after :meth:`close`.

        The signal sender is the consumer.
        """
        return blinker.Signal(doc='Emitted after the consumer is closed.')

    def start(self, block=True):
        """Start discovering and listing to connections."""
        if self._state == INIT:
            if not any(self.on_message.receivers_for(blinker.ANY)):
                raise RuntimeError('no receivers connected to on_message')

            self.logger.debug('starting %s...', self.name)
            self._state = RUNNING
            self.query_nsqd()

            if self.lookupds:
                self.query_lookupd()
                self._killables.add(self._workers.spawn(self._poll_lookupd))

            self._killables.add(self._workers.spawn(self._poll_ready))

        else:
            self.logger.warn('%s already started', self.name)

        if block:
            self.join()

    def close(self):
        """Immediately close all connections and stop workers."""
        if not self.is_running:
            return

        self._state = CLOSED

        self.logger.debug('killing %d worker(s)', len(self._killables))
        self._killables.kill(block=False)

        self.logger.debug('closing %d connection(s)', len(self._connections))
        for conn in self._connections:
            conn.close_stream()

        self.on_close.send(self)

    def join(self, timeout=None, raise_error=False):
        """Block until all connections have closed and workers stopped."""
        self._workers.join(timeout, raise_error)

    @property
    def is_running(self):
        """Check if consumer is currently running."""
        return self._state == RUNNING

    @property
    def is_starved(self):
        """Evaluate whether any of the connections are starved.

        This property should be used by message handlers to reliably identify
        when to process a batch of messages.
        """
        return any(conn.is_starved for conn in self._connections)

    @property
    def total_ready_count(self):
        return sum(c.ready_count for c in self._connections)

    @property
    def total_in_flight(self):
        return sum(c.in_flight for c in self._connections)

    def query_nsqd(self):
        self.logger.debug('querying nsqd...')
        for address in self.nsqd_tcp_addresses:
            address, port = address.split(':')
            self.connect_to_nsqd(address, int(port))

    def query_lookupd(self):
        self.logger.debug('querying lookupd...')
        lookupd = next(self.iterlookupds)

        try:
            producers = lookupd.lookup(self.topic)['producers']
            self.logger.debug('found %d producers', len(producers))

        except Exception as error:
            self.logger.warn(
                'Failed to lookup %s on %s (%s)',
                self.topic, lookupd.address, error)
            return

        for producer in producers:
            self.connect_to_nsqd(
                producer['broadcast_address'], producer['tcp_port'])

    def _poll_lookupd(self):
        try:
            delay = self.lookupd_poll_interval * self.lookupd_poll_jitter
            gevent.sleep(random.random() * delay)

            while True:
                gevent.sleep(self.lookupd_poll_interval)
                self.query_lookupd()

        except gevent.GreenletExit:
            pass

    def _poll_ready(self):
        try:
            while True:
                if self._redistributed_ready_event.wait(5):
                    self._redistributed_ready_event.clear()
                self._redistribute_ready_state()

        except gevent.GreenletExit:
            pass

    def _redistribute_ready_state(self):
        if not self.is_running:
            return

        if len(self._connections) > self.max_in_flight:
            ready_state = self._get_unsaturated_ready_state()
        else:
            ready_state = self._get_saturated_ready_state()

        for conn, count in ready_state.items():
            if conn.ready_count == count:
                self.logger.debug('[%s] RDY count already %d', conn, count)
                continue

            self.logger.debug('[%s] sending RDY %d', conn, count)

            try:
                conn.ready(count)
            except NSQSocketError as error:
                self.logger.warn('[%s] RDY %d failed (%r)', conn, count, error)

    def _get_unsaturated_ready_state(self):
        ready_state = {}
        active = []

        for conn, state in self._connections.items():
            if state == BACKOFF:
                ready_state[conn] = 0

            else:
                active.append(conn)

        random.shuffle(active)

        for conn in active[self.max_in_flight:]:
            ready_state[conn] = 0

        for conn in active[:self.max_in_flight]:
            ready_state[conn] = 1

        return ready_state

    def _get_saturated_ready_state(self):
        ready_state = {}
        active = []
        now = time.time()

        for conn, state in self._connections.items():
            if state == BACKOFF:
                ready_state[conn] = 0

            elif state in (INIT, THROTTLED):
                ready_state[conn] = 1

            elif (now - conn.last_message) > self.low_ready_idle_timeout:
                self.logger.info(
                    '[%s] idle connection, giving up RDY count', conn)
                ready_state[conn] = 1

            else:
                active.append(conn)

        if not active:
            return ready_state

        ready_available = self.max_in_flight - sum(ready_state.values())
        connection_max_in_flight = ready_available // len(active)

        for conn in active:
            ready_state[conn] = connection_max_in_flight

        for conn in random.sample(active, ready_available % len(active)):
            ready_state[conn] += 1

        return ready_state

    def redistribute_ready_state(self):
        self._redistributed_ready_event.set()

    def connect_to_nsqd(self, address, port):
        if not self.is_running:
            return

        conn = NsqdTCPClient(address, port, **self.conn_kwargs)
        if conn in self._connections:
            self.logger.debug('[%s] already connected', conn)
            return

        self._connections[conn] = INIT
        self.logger.debug('[%s] connecting...', conn)

        conn.on_message.connect(self.handle_message)
        conn.on_response.connect(self.handle_response)
        conn.on_error.connect(self.handle_error)
        conn.on_finish.connect(self.handle_finish)
        conn.on_requeue.connect(self.handle_requeue)
        conn.on_auth.connect(self.handle_auth)

        try:
            conn.connect()
            conn.identify()

            if conn.max_ready_count < self.max_in_flight:
                msg = (
                    '[%s] max RDY count %d < consumer max in flight %d, '
                    'truncation possible')

                self.logger.warning(
                    msg, conn, conn.max_ready_count, self.max_in_flight)

            conn.subscribe(self.topic, self.channel)

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
        self._workers.spawn(self._listen, conn)
        self.redistribute_ready_state()

        if str(conn) not in self.nsqd_tcp_addresses:
            return

        self._connection_backoffs[conn].success()

    def handle_connection_failure(self, conn):
        del self._connections[conn]
        conn.close_stream()

        if not self.is_running:
            return

        self.redistribute_ready_state()

        if str(conn) not in self.nsqd_tcp_addresses:
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
        self.on_response.send(self, response=response)

    def handle_error(self, conn, error):
        self.logger.debug('[%s] error: %s', conn, error)
        self.on_error.send(self, error=error)

    def _handle_message(self, message):
        if self.max_tries and message.attempts > self.max_tries:
            self.logger.warning(
                "giving up on message '%s' after max tries %d",
                message.id, self.max_tries)
            self.on_giving_up.send(self, message=message)
            return message.finish()

        self.on_message.send(self, message=message)

        if not self.is_running:
            return

        if message.is_async():
            return

        if message.has_responded():
            return

        message.finish()

    def handle_message(self, conn, message):
        self.logger.debug('[%s] got message: %s', conn, message.id)

        try:
            return self._handle_message(message)

        except NSQRequeueMessage as error:
            if error.backoff is None:
                backoff = self.backoff_on_requeue
            else:
                backoff = error.backoff

        except Exception as error:
            backoff = True
            self.logger.exception(
                '[%s] caught exception while handling message', conn)
            self.on_exception.send(self, message=message, error=error)

        if not self.is_running:
            return

        if message.has_responded():
            return

        try:
            message.requeue(self.requeue_delay, backoff)
        except NSQException as error:
            self.logger.warning(
                '[%s] error requeueing message (%r)', conn, error)

    def _create_backoff(self):
        return BackoffTimer(max_interval=self.max_backoff_duration)

    def _start_backoff(self, conn):
        self._connections[conn] = BACKOFF

        interval = self._message_backoffs[conn].get_interval()
        gevent.spawn_later(interval, self._start_throttled, conn)

        self.logger.info('[%s] backing off for %s seconds', conn, interval)
        self.redistribute_ready_state()

    def _start_throttled(self, conn):
        if self._connections.get(conn) != BACKOFF:
            return

        self._connections[conn] = THROTTLED
        self.logger.info('[%s] testing backoff state with RDY 1', conn)
        self.redistribute_ready_state()

    def _complete_backoff(self, conn):
        if self._message_backoffs[conn].is_reset():
            self._connections[conn] = RUNNING
            self.logger.info('backoff complete, resuming normal operation')
            self.redistribute_ready_state()
        else:
            self._start_backoff(conn)

    def _finish_message(self, conn, backoff):
        if not self.max_backoff_duration:
            return

        try:
            state = self._connections[conn]
        except KeyError:
            return

        if state == BACKOFF:
            return

        if backoff:
            self._message_backoffs[conn].failure()
            self._start_backoff(conn)

        elif state == THROTTLED:
            self._message_backoffs[conn].success()
            self._complete_backoff(conn)

        elif state == INIT:
            self._connections[conn] = RUNNING
            self.redistribute_ready_state()

    def handle_finish(self, conn, message_id):
        self.logger.debug('[%s] finished message: %s', conn, message_id)
        self._finish_message(conn, backoff=False)
        self.on_finish.send(self, message_id=message_id)

    def handle_requeue(self, conn, message_id, timeout, backoff):
        self.logger.debug(
            '[%s] requeued message: %s (%s)', conn, message_id, timeout)
        self._finish_message(conn, backoff=backoff)
        self.on_requeue.send(self, message_id=message_id, timeout=timeout)
