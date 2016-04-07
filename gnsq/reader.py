# -*- coding: utf-8 -*-
from __future__ import absolute_import, division

import logging
import random
import gevent
import blinker
import time
import six

from itertools import cycle
from collections import defaultdict
from multiprocessing import cpu_count
from gevent.queue import Queue
from six.moves import range

from .lookupd import Lookupd
from .nsqd import Nsqd
from .backofftimer import BackoffTimer
from .states import INIT, RUNNING, BACKOFF, THROTTLED, CLOSED
from .decorators import cached_property

from .errors import (
    NSQException,
    NSQNoConnections,
    NSQRequeueMessage,
    NSQSocketError,
)


class Reader(object):
    """High level NSQ consumer.

    A Reader will connect to the nsqd tcp addresses or poll the provided
    nsqlookupd http addresses for the configured topic and send signals to
    message handlers connected to the `on_message` signal or provided by
    `message_handler`.

    Messages will automatically be finished when the message handle returns
    unless the readers `async` flag is set to `True`. If an exception occurs or
    :class:`gnsq.errors.NSQRequeueMessage` is raised, the message will be
    requeued.

    The Reader will handle backing off of failed messages up to a configurable
    `max_interval` as well as automatically reconnecting to dropped connections.

    :param topic: specifies the desired NSQ topic

    :param channel: specifies the desired NSQ channel

    :param nsqd_tcp_addresses: a sequence of string addresses of the nsqd
        instances this reader should connect to

    :param lookupd_http_addresses: a sequence of string addresses of the
        nsqlookupd instances this reader should query for producers of the
        specified topic

    :param name: a string that is used for logging messages (defaults to
        'gnsq.reader.topic.channel')

    :param message_handler: the callable that will be executed for each message
        received

    :param async: consider the message handling to be async. The message will
        not automatically be finished after the handler returns and must
        manually be called

    :param max_tries: the maximum number of attempts the reader will make to
        process a message after which messages will be automatically discarded

    :param max_in_flight: the maximum number of messages this reader will
        pipeline for processing. this value will be divided evenly amongst the
        configured/discovered nsqd producers

    :param max_concurrency: the maximum number of messages that will be handled
        concurrently. Defaults to the number of nsqd connections. Setting
        `max_concurrency` to `-1` will use the systems cpu count.

    :param requeue_delay: the default delay to use when requeueing a failed
        message

    :param lookupd_poll_interval: the amount of time in seconds between querying
        all of the supplied nsqlookupd instances.  A random amount of time based
        on this value will be initially introduced in order to add jitter when
        multiple readers are running

    :param lookupd_poll_jitter: the maximum fractional amount of jitter to add
        to the lookupd pool loop. This helps evenly distribute requests even if
        multiple consumers restart at the same time.

    :param low_ready_idle_timeout: the amount of time in seconds to wait for a
        message from a producer when in a state where RDY counts are
        re-distributed (ie. max_in_flight < num_producers)

    :param max_backoff_duration: the maximum time we will allow a backoff state
        to last in seconds. If zero, backoff wil not occur

    :param backoff_on_requeue: if False, backoff will only occur on exception

    :param \*\*kwargs: passed to :class:`gnsq.Nsqd` initialization
    """
    def __init__(
        self,
        topic,
        channel,
        nsqd_tcp_addresses=[],
        lookupd_http_addresses=[],
        name=None,
        message_handler=None,
        async=False,
        max_tries=5,
        max_in_flight=1,
        max_concurrency=0,
        requeue_delay=0,
        lookupd_poll_interval=60,
        lookupd_poll_jitter=0.3,
        low_ready_idle_timeout=10,
        max_backoff_duration=128,
        backoff_on_requeue=True,
        **kwargs
    ):
        if not nsqd_tcp_addresses and not lookupd_http_addresses:
            raise ValueError('must specify at least on nsqd or lookupd')

        nsqd_tcp_addresses = self._get_nsqds(nsqd_tcp_addresses)
        lookupd_http_addresses = self._get_lookupds(lookupd_http_addresses)
        random.shuffle(lookupd_http_addresses)

        self.nsqd_tcp_addresses = nsqd_tcp_addresses
        self.lookupds = [Lookupd(a) for a in lookupd_http_addresses]
        self.iterlookupds = cycle(self.lookupds)

        self.topic = topic
        self.channel = channel
        self.async = async
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

        if max_concurrency < 0:
            self.max_concurrency = cpu_count()
        else:
            self.max_concurrency = max_concurrency

        if max_concurrency:
            self.queue = Queue()
        else:
            self.queue = None

        self._need_ready_redistributed = False
        self.last_random_ready = time.time()
        self.state = INIT

        self.logger = logging.getLogger(self.name)
        self.conn_backoffs = defaultdict(self.create_backoff)
        self.backoff = self.create_backoff()

        self.conns = set()
        self.pending = set()

        self.workers = []
        self.conn_workers = {}

    @cached_property
    def on_message(self):
        """Emitted when a message is received.

        The signal sender is the reader and the `message` is sent as an
        argument. The `message_handler` param is connected to this signal.
        """
        return blinker.Signal(doc='Emitted when a message is received.')

    @cached_property
    def on_response(self):
        """Emitted when a response is received.

        The signal sender is the reader and the `response` is sent as an
        argument.
        """
        return blinker.Signal(doc='Emitted when a response is received.')

    @cached_property
    def on_error(self):
        """Emitted when an error is received.

        The signal sender is the reader and the `error` is sent as an
        argument.
        """
        return blinker.Signal(doc='Emitted when a error is received.')

    @cached_property
    def on_finish(self):
        """Emitted after a message is successfully finished.

        The signal sender is the reader and the `message_id` is sent as an
        argument.
        """
        return blinker.Signal(doc='Emitted after the a message is finished.')

    @cached_property
    def on_requeue(self):
        """Emitted after a message is requeued.

        The signal sender is the reader and the `message_id` and `timeout`
        are sent as arguments.
        """
        return blinker.Signal(doc='Emitted after the a message is requeued.')

    @cached_property
    def on_giving_up(self):
        """Emitted after a giving up on a message.

        Emitted when a message has exceeded the maximum number of attempts
        (`max_tries`) and will no longer be requeued. This is useful to perform
        tasks such as writing to disk, collecting statistics etc. The signal
        sender is the reader and the `message` is sent as an argument.
        """
        return blinker.Signal(doc='Sent after a giving up on a message.')

    @cached_property
    def on_auth(self):
        """Emitted after a connection is successfully authenticated.

        The signal sender is the reader and the `conn` and parsed `response` are
        sent as arguments.
        """
        return blinker.Signal(doc='Emitted when a response is received.')

    @cached_property
    def on_exception(self):
        """Emitted when an exception is caught while handling a message.

        The signal sender is the reader and the `message` and `error` are sent
        as arguments.
        """
        return blinker.Signal(doc='Emitted when an exception is caught.')

    @cached_property
    def on_close(self):
        """Emitted after :meth:`close`.

        The signal sender is the reader.
        """
        return blinker.Signal(doc='Emitted after the reader is closed.')

    def _get_nsqds(self, nsqd_tcp_addresses):
        if isinstance(nsqd_tcp_addresses, six.string_types):
            return set([nsqd_tcp_addresses])

        elif isinstance(nsqd_tcp_addresses, (list, tuple, set)):
            return set(nsqd_tcp_addresses)

        raise TypeError('nsqd_tcp_addresses must be a list, set or tuple')

    def _get_lookupds(self, lookupd_http_addresses):
        if isinstance(lookupd_http_addresses, six.string_types):
            return [lookupd_http_addresses]

        elif isinstance(lookupd_http_addresses, (list, tuple)):
            lookupd_http_addresses = list(lookupd_http_addresses)
            return lookupd_http_addresses

        msg = 'lookupd_http_addresses must be a list, set or tuple'
        raise TypeError(msg)

    def start(self, block=True):
        """Start discovering and listing to connections."""
        if self.state != INIT:
            self.logger.warn('%s all ready started' % self.name)
            if block:
                self.join()
            return

        if not any(self.on_message.receivers_for(blinker.ANY)):
            raise RuntimeError('no receivers connected to on_message')

        self.logger.debug('starting %s...' % self.name)
        self.state = RUNNING
        self.query_nsqd()

        if self.lookupds:
            self.query_lookupd()
            self.workers.append(gevent.spawn(self._poll_lookupd))

        self.workers.append(gevent.spawn(self._poll_ready))

        for _ in range(self.max_concurrency):
            self.workers.append(gevent.spawn(self._run))

        if block:
            self.join()

    def close(self):
        """Immediately close all connections and stop workers."""
        if not self.is_running:
            return

        self.state = CLOSED

        self.logger.debug('closing %d worker(s)' % len(self.workers))
        gevent.killall(self.workers, block=False)

        self.logger.debug('closing %d connection(s)' % len(self.conns))
        for conn in self.conns:
            conn.close_stream()

        self.on_close.send(self)

    def join(self, timeout=None, raise_error=False):
        """Block until all connections have closed and workers stopped."""
        gevent.joinall(self.workers, timeout, raise_error)
        gevent.joinall(list(self.conn_workers.values()), timeout, raise_error)

    @property
    def is_running(self):
        """Check if reader is currently running."""
        return self.state in (RUNNING, BACKOFF, THROTTLED)

    @property
    def is_starved(self):
        """Evaluate whether any of the connections are starved.

        This property should be used by message handlers to reliably identify
        when to process a batch of messages.
        """
        return any(conn.is_starved for conn in self.conns)

    @property
    def connection_max_in_flight(self):
        return max(1, self.max_in_flight // max(1, len(self.conns)))

    @property
    def total_ready_count(self):
        return sum(c.ready_count for c in self.conns)

    @property
    def total_in_flight(self):
        return sum(c.in_flight for c in self.conns)

    @property
    def total_in_flight_or_ready(self):
        return self.total_in_flight + self.total_ready_count

    def send_ready(self, conn, count):
        if self.state == CLOSED:
            self.logger.debug('[%s] cannot send RDY (in state CLOSED)' % conn)
            return

        if self.state == BACKOFF:
            self.logger.debug('[%s] cannot send RDY (in state BACKOFF)' % conn)
            return

        if self.state == THROTTLED and self.total_in_flight_or_ready:
            msg = '[%s] cannot send RDY (THROTTLED and %d in flight or ready)'
            self.logger.debug(msg % (conn, self.total_in_flight_or_ready))
            return

        if not conn.is_connected:
            self.logger.debug('[%s] cannot send RDY (connection closed)' % conn)
            return

        total = self.total_ready_count - conn.ready_count + count
        if total > self.max_in_flight:
            if not (conn.ready_count or conn.in_flight):
                self.logger.debug('[%s] sending later' % conn)
                gevent.spawn_later(5, self.send_ready, conn, count)
            return

        self.logger.debug('[%s] sending RDY %d' % (conn, count))

        try:
            conn.ready(count)
        except NSQSocketError as error:
            self.logger.warn('[%s] RDY %d failed (%r)' % (conn, count, error))

    def query_nsqd(self):
        self.logger.debug('querying nsqd...')
        for address in self.nsqd_tcp_addresses:
            address, port = address.split(':')
            conn = Nsqd(address, int(port), **self.conn_kwargs)
            self.connect_to_nsqd(conn)

    def query_lookupd(self):
        self.logger.debug('querying lookupd...')
        lookupd = next(self.iterlookupds)

        try:
            producers = lookupd.lookup(self.topic)['producers']
            self.logger.debug('found %d producers' % len(producers))

        except Exception as error:
            msg = 'Failed to lookup %s on %s (%s)'
            self.logger.warn(msg % (self.topic, lookupd.address, error))
            return

        for producer in producers:
            conn = Nsqd(
                producer.get('broadcast_address') or producer['address'],
                producer['tcp_port'],
                producer['http_port'],
                **self.conn_kwargs
            )
            self.connect_to_nsqd(conn)

    def create_backoff(self):
        return BackoffTimer(max_interval=self.max_backoff_duration)

    def start_backoff(self):
        self.state = BACKOFF

        for conn in self.conns:
            try:
                conn.ready(0)
            except NSQSocketError as error:
                self.logger.warn('[%s] RDY 0 failed (%r)' % (conn, error))

        interval = self.backoff.get_interval()
        self.logger.info('backing off for %s seconds' % interval)
        gevent.sleep(interval)

        self.state = THROTTLED

        conn = self.random_connection()
        if not conn:
            return

        self.logger.info('[%s] testing backoff state with RDY 1' % conn)
        self.send_ready(conn, 1)

    def complete_backoff(self):
        self.state = RUNNING
        self.logger.info('backoff complete, resuming normal operation')

        count = self.connection_max_in_flight
        for conn in self.conns:
            self.send_ready(conn, count)

    def _poll_lookupd(self):
        delay = self.lookupd_poll_interval * self.lookupd_poll_jitter
        gevent.sleep(random.random() * delay)

        while True:
            gevent.sleep(self.lookupd_poll_interval)
            self.query_lookupd()

    def _poll_ready(self):
        while True:
            gevent.sleep(5)
            if not self.need_ready_redistributed:
                continue
            self.redistribute_ready_state()

    @property
    def need_ready_redistributed(self):
        if self.state == BACKOFF:
            return False

        if self._need_ready_redistributed:
            return True

        if len(self.conns) > self.max_in_flight:
            return True

        if self.state == THROTTLED and len(self.conns) > 1:
            return True

    @need_ready_redistributed.setter
    def need_ready_redistributed(self, value):
        self._need_ready_redistributed = value

    def redistribute_ready_state(self):
        self.need_ready_redistributed = False

        # first set RDY 0 to all connections that have not received a message
        # within a configurable timeframe (low_ready_idle_timeout).
        for conn in self.conns:
            if conn.ready_count == 0:
                continue

            if (time.time() - conn.last_message) < self.low_ready_idle_timeout:
                continue

            self.logger.info('[%s] idle connection, giving up RDY count' % conn)
            try:
                conn.ready(0)
            except NSQSocketError as error:
                self.logger.warn('[%s] RDY 0 failed (%r)' % (conn, error))

        if self.state == THROTTLED:
            max_in_flight = 1 - self.total_in_flight_or_ready
        else:
            max_in_flight = self.max_in_flight - self.total_ready_count

        if max_in_flight <= 0:
            return

        # randomly walk the list of possible connections and send RDY 1 (up to
        # our calculate "max_in_flight").  We only need to send RDY 1 because in
        # both cases described above your per connection RDY count would never
        # be higher.
        #
        # We also don't attempt to avoid the connections who previously might
        # have had RDY 1 because it would be overly complicated and not actually
        # worth it (ie. given enough redistribution rounds it doesn't matter).
        conns = list(self.conns)
        conns = random.sample(conns, min(max_in_flight, len(self.conns)))

        for conn in conns:
            self.logger.info('[%s] redistributing RDY' % conn)
            self.send_ready(conn, 1)

    def random_ready_conn(self, conn):
        # if all connections aren't getting RDY
        # occsionally randomize which connection gets RDY
        if len(self.conns) <= self.max_in_flight:
            return conn

        if (time.time() - self.last_random_ready) < 30:
            return conn

        ready_conns = [c for c in self.conns if not c.ready_count]
        if not ready_conns:
            return conn

        self.last_random_ready = time.time()
        return random.choice(ready_conns)

    def update_ready(self, conn):
        if self.state in (BACKOFF, THROTTLED):
            return

        conn = self.random_ready_conn(conn)
        if conn.ready_count >= max(conn.last_ready * 0.25, 2):
            return

        self.send_ready(conn, self.connection_max_in_flight)

    def random_connection(self):
        if not self.conns:
            return None
        return random.choice(list(self.conns))

    def publish(self, topic, message):
        """Publish a message to a random connection."""
        conn = self.random_connection()
        if conn is None:
            raise NSQNoConnections()

        conn.publish(topic, message)

    def connect_to_nsqd(self, conn):
        if not self.is_running:
            return

        if conn in self.conns:
            self.logger.debug('[%s] already connected' % conn)
            return

        if conn in self.pending:
            self.logger.debug('[%s] already pending' % conn)
            return

        self.logger.debug('[%s] connecting...' % conn)

        conn.on_response.connect(self.handle_response)
        conn.on_error.connect(self.handle_error)
        conn.on_finish.connect(self.handle_finish)
        conn.on_requeue.connect(self.handle_requeue)
        conn.on_auth.connect(self.handle_auth)

        if self.max_concurrency:
            conn.on_message.connect(self.queue_message)
        else:
            conn.on_message.connect(self.handle_message)

        self.pending.add(conn)

        try:
            conn.connect()
            conn.identify()

            if conn.max_ready_count < self.max_in_flight:
                msg = ' '.join([
                    '[%s] max RDY count %d < reader max in flight %d,',
                    'truncation possible'
                ])

                self.logger.warning(msg % (
                    conn,
                    conn.max_ready_count,
                    self.max_in_flight
                ))

            conn.subscribe(self.topic, self.channel)
            self.send_ready(conn, 1)

        except NSQException as error:
            self.logger.warn('[%s] connection failed (%r)' % (conn, error))
            self.handle_connection_failure(conn)
            return

        finally:
            self.pending.remove(conn)

        # Check if we've closed since we started
        if not self.is_running:
            conn.close_stream()
            return

        self.logger.info('[%s] connection successful' % conn)
        self.handle_connection_success(conn)

    def _listen(self, conn):
        try:
            conn.listen()
        except NSQException as error:
            self.logger.warning('[%s] connection lost (%r)' % (conn, error))

        self.handle_connection_failure(conn)

    def _run(self):
        for conn, message in self.queue:
            if not conn.is_connected:
                continue
            self.handle_message(conn, message)

    def queue_message(self, conn, message):
        self.logger.debug('[%s] queueing message: %s' % (conn, message.id))
        self.queue.put((conn, message))

    def handle_connection_success(self, conn):
        self.conns.add(conn)
        self.conn_workers[conn] = gevent.spawn(self._listen, conn)

        if str(conn) not in self.nsqd_tcp_addresses:
            return

        self.conn_backoffs[conn].success()

    def handle_connection_failure(self, conn):
        self.conns.discard(conn)
        self.conn_workers.pop(conn, None)
        conn.close_stream()

        if self.state == CLOSED:
            return

        if conn.ready_count:
            self.need_ready_redistributed = True

        if str(conn) not in self.nsqd_tcp_addresses:
            return

        seconds = self.conn_backoffs[conn].failure().get_interval()
        self.logger.debug('[%s] retrying in %ss' % (conn, seconds))
        gevent.spawn_later(seconds, self.connect_to_nsqd, conn)

    def handle_response(self, conn, response):
        self.logger.debug('[%s] response: %s' % (conn, response))
        self.on_response.send(self, response=response)

    def handle_error(self, conn, error):
        self.logger.debug('[%s] error: %s' % (conn, error))
        self.on_error.send(self, error=error)

    def _handle_message(self, message):
        if self.max_tries and message.attempts > self.max_tries:
            msg = "giving up on message '%s' after max tries %d"
            self.logger.warning(msg % (message.id, self.max_tries))
            self.on_giving_up.send(self, message=message)
            return message.finish()

        self.on_message.send(self, message=message)

        if not self.is_running:
            return

        if self.async:
            return

        if message.has_responded():
            return

        message.finish()

    def handle_message(self, conn, message):
        self.logger.debug('[%s] got message: %s' % (conn, message.id))
        self.update_ready(conn)

        try:
            return self._handle_message(message)

        except NSQRequeueMessage as error:
            if error.backoff is None:
                backoff = self.backoff_on_requeue
            else:
                backoff = error.backoff

        except Exception as error:
            backoff = True
            msg = '[%s] caught exception while handling message' % conn
            self.logger.exception(msg)
            self.on_exception.send(self, message=message, error=error)

        if not self.is_running:
            return

        if message.has_responded():
            return

        try:
            message.requeue(self.requeue_delay, backoff)
        except NSQException as error:
            msg = '[%s] error requeueing message (%r)' % (conn, error)
            self.logger.warning(msg)

    def handle_finish(self, conn, message_id):
        self.logger.debug('[%s] finished message: %s' % (conn, message_id))
        self.handle_backoff(False)
        self.update_ready(conn)
        self.on_finish.send(self, message_id=message_id)

    def handle_requeue(self, conn, message_id, timeout, backoff):
        msg = '[%s] requeued message: %s (%s)'
        self.logger.debug(msg % (conn, message_id, timeout))
        self.handle_backoff(backoff)
        self.update_ready(conn)
        self.on_requeue.send(self, message_id=message_id, timeout=timeout)

    def handle_auth(self, conn, response):
        metadata = []
        if response.get('identity'):
            metadata.append("Identity: %r" % response['identity'])

        if response.get('permission_count'):
            metadata.append("Permissions: %d" % response['permission_count'])

        if response.get('identity_url'):
            metadata.append(response['identity_url'])

        self.logger.info('[%s] AUTH accepted %s' % (conn, ' '.join(metadata)))
        self.on_auth.send(self, conn=conn, response=response)

    def handle_backoff(self, backoff):
        if not self.max_backoff_duration:
            return

        if self.state in (BACKOFF, CLOSED):
            return

        if backoff:
            self.backoff.failure()
        else:
            self.backoff.success()

        if self.state == THROTTLED and self.backoff.is_reset():
            return self.complete_backoff()

        if not self.backoff.is_reset():
            return self.start_backoff()
