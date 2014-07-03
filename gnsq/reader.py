import logging
import random
import gevent
import blinker
import time

from itertools import cycle
from collections import defaultdict

from .lookupd import Lookupd
from .nsqd import Nsqd
from .backofftimer import BackoffTimer
from .states import INIT, RUNNING, BACKOFF, THROTTLED, CLOSED

from .errors import (
    NSQException,
    NSQNoConnections,
    NSQRequeueMessage
)


class Reader(object):
    def __init__(
        self,
        topic,
        channel,
        nsqd_tcp_addresses=[],
        lookupd_http_addresses=[],
        name=None,
        async=False,
        max_tries=5,
        max_in_flight=1,
        requeue_delay=0,
        lookupd_poll_interval=60,
        lookupd_poll_jitter=0.3,
        low_ready_idle_timeout=10,
        max_backoff_duration=128,
        **kwargs
    ):
        if not nsqd_tcp_addresses and not lookupd_http_addresses:
            raise ValueError('must specify at least on nsqd or lookupd')

        if isinstance(nsqd_tcp_addresses, basestring):
            self.nsqd_tcp_addresses = [nsqd_tcp_addresses]
        elif isinstance(nsqd_tcp_addresses, (list, tuple)):
            self.nsqd_tcp_addresses = set(nsqd_tcp_addresses)
        else:
            raise TypeError('nsqd_tcp_addresses must be a list, set or tuple')

        if isinstance(lookupd_http_addresses, basestring):
            lookupd_http_addresses = [lookupd_http_addresses]
        elif isinstance(lookupd_http_addresses, (list, tuple)):
            lookupd_http_addresses = list(lookupd_http_addresses)
            random.shuffle(lookupd_http_addresses)
        else:
            msg = 'lookupd_http_addresses must be a list, set or tuple'
            raise TypeError(msg)

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
        self.max_backoff_duration = max_backoff_duration
        self.conn_kwargs = kwargs

        if name:
            self.name = name
        else:
            self.name = '{}.{}.{}'.format(__name__, self.topic, self.channel)

        self._need_ready_redistributed = False
        self.last_random_ready = time.time()
        self.state = INIT

        self.logger = logging.getLogger(self.name)
        self.conn_backoffs = defaultdict(self.create_backoff)
        self.backoff = self.create_backoff()

        self.on_response = blinker.Signal()
        self.on_error = blinker.Signal()
        self.on_message = blinker.Signal()
        self.on_finish = blinker.Signal()
        self.on_requeue = blinker.Signal()
        self.on_giving_up = blinker.Signal()
        self.on_exception = blinker.Signal()

        self.conns = set()
        self.pending = set()

        self.workers = []
        self.conn_workers = {}

    def start(self, block=True):
        if self.state != INIT:
            self.logger.debug('{} all ready started'.format(self.name))
            return

        self.logger.debug('starting {}...'.format(self.name))
        self.state = RUNNING
        self.query_nsqd()

        if self.lookupds:
            self.query_lookupd()
            self.workers.append(gevent.spawn(self._poll_lookupd))

        self.workers.append(gevent.spawn(self._poll_ready))

        if block:
            self.join()

    def close(self):
        if not self.is_running:
            return

        self.state = CLOSED

        for worker in self.workers:
            worker.kill()

        for conn in self.conns:
            conn.close_stream()

    def join(self, timeout=None, raise_error=False):
        gevent.joinall(self.workers, timeout, raise_error)
        gevent.joinall(self.conn_workers.values(), timeout, raise_error)

    @property
    def is_running(self):
        return self.state in (RUNNING, BACKOFF, THROTTLED)

    @property
    def is_starved(self):
        for conn in self.conns:
            if conn.in_flight >= max(conn.last_ready * 0.85, 1):
                return True
        return False

    @property
    def connection_max_in_flight(self):
        return max(1, self.max_in_flight / max(1, len(self.conns)))

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
        if self.state == BACKOFF:
            return

        if self.state == THROTTLED and self.total_in_flight_or_ready:
            return

        if (self.total_in_flight_or_ready + count) > self.max_in_flight:
            if not (conn.ready_count or conn.in_flight):
                gevent.spawn_later(5, self.send_ready, conn, count)
            return

        conn.ready(count)

    def query_nsqd(self):
        self.logger.debug('querying nsqd...')
        for address in self.nsqd_tcp_addresses:
            address, port = address.split(':')
            conn = Nsqd(address, int(port), **self.conn_kwargs)
            self.connect_to_nsqd(conn)

    def query_lookupd(self):
        self.logger.debug('querying lookupd...')
        lookupd = self.iterlookupds.next()

        try:
            producers = lookupd.lookup(self.topic)['producers']
            self.logger.debug('found {} producers'.format(len(producers)))

        except Exception as error:
            msg = 'Failed to lookup {} on {} ({})'
            self.logger.warn(msg.format(self.topic, lookupd.address, error))
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
            conn.ready(0)

        interval = self.backoff.get_interval()
        self.logger.info('backing off for {} seconds'.format(interval))
        gevent.sleep(interval)

        self.state = THROTTLED
        if not self.conns:
            return

        conn = self.random_connection()
        self.logger.info('[{}] testing backoff state with RDY 1'.format(conn))
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

            msg = '[{}] idle connection, giving up RDY count'.format(conn)
            self.logger.info(msg)

            conn.ready(0)

        if self.state == THROTTLED:
            max_in_flight = 1 - self.total_in_flight_or_ready
        else:
            max_in_flight = self.max_in_flight - self.total_in_flight_or_ready

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
            self.logger.info('[{}] redistributing RDY'.format(conn))
            self.send_ready(conn, 1)

    def random_ready_conn(self, conn):
        # if all connections aren't getting RDY
        # occsionally randomize which connection gets RDY
        if len(self.conns) <= self.max_in_flight:
            return conn

        if (time.time() - self.last_random_ready) < 30:
            return conn

        self.last_random_ready = time.time()
        return random.choice([c for c in self.conns if not c.ready_count])

    def update_ready(self, conn):
        if self.state in (BACKOFF, THROTTLED):
            return

        conn = self.random_ready_conn(conn)
        if conn.ready_count < max(conn.last_ready * 0.25, 2):
            self.send_ready(conn, self.connection_max_in_flight)

    def random_connection(self):
        if not self.conns:
            return None
        return random.choice(list(self.conns))

    def publish(self, topic, message):
        conn = self.random_connection()
        if conn is None:
            raise NSQNoConnections()

        conn.publish(topic, message)

    def connect_to_nsqd(self, conn):
        if not self.is_running:
            return

        if conn in self.conns:
            self.logger.debug('[{}] already connected'.format(conn))
            return

        if conn in self.pending:
            self.logger.debug('[{}] already pending'.format(conn))
            return

        self.logger.debug('[{}] connecting...'.format(conn))

        conn.on_response.connect(self.handle_response)
        conn.on_error.connect(self.handle_error)
        conn.on_message.connect(self.handle_message)
        conn.on_finish.connect(self.handle_finish)
        conn.on_requeue.connect(self.handle_requeue)

        self.pending.add(conn)

        try:
            conn.connect()
            conn.identify()

            if conn.max_ready_count < self.max_in_flight:
                self.logger.warning(' '.join([
                    '[{}] max RDY count {} < reader max in flight {},',
                    'truncation possible'
                ]).format(conn, conn.max_ready_count, self.max_in_flight))

            conn.subscribe(self.topic, self.channel)
            self.send_ready(conn, 1)

        except NSQException as error:
            msg = '[{}] connection failed ({!r})'.format(conn, error)
            self.logger.debug(msg)
            self.handle_connection_failure(conn)
            return

        finally:
            self.pending.remove(conn)

        # Check if we've closed since we started
        if not self.is_running:
            conn.close_stream()
            return

        self.logger.info('[{}] connection successful'.format(conn))
        self.handle_connection_success(conn)

    def _listen(self, conn):
        try:
            conn.listen()
        except NSQException as error:
            if self.state == CLOSED:
                return
            msg = '[{}] connection lost ({!r})'.format(conn, error)
            self.logger.warning(msg)

        self.handle_connection_failure(conn)

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

        if conn.ready_count:
            self.need_ready_redistributed = True

        if str(conn) not in self.nsqd_tcp_addresses:
            return

        seconds = self.conn_backoffs[conn].failure().get_interval()
        self.logger.debug('[{}] retrying in {}s'.format(conn, seconds))
        gevent.spawn_later(seconds, self.connect_to_nsqd, conn)

    def handle_response(self, conn, response):
        self.logger.debug('[{}] response: {}'.format(conn, response))
        self.on_response.send(self, response=response)

    def handle_error(self, conn, error):
        self.logger.debug('[{}] error: {}'.format(conn, error))
        self.on_error.send(self, error=error)

    def handle_message(self, conn, message):
        self.logger.debug('[{}] got message: {}'.format(conn, message.id))

        if self.max_tries and message.attempts > self.max_tries:
            msg = "giving up on message '{}' after max tries {}"
            self.logger.warning(msg.format(message.id, self.max_tries))
            self.on_giving_up.send(self, conn, message)
            return message.finish()

        try:
            self.on_message.send(self, message=message)
            if not self.async:
                message.finish()
            return

        except NSQRequeueMessage:
            pass

        except Exception as error:
            msg = '[{}] caught exception while handling message'.format(conn)
            self.logger.exception(msg)
            self.on_exception.send(self, message=message, error=error)

        message.requeue(self.requeue_delay)

    def handle_finish(self, conn, message_id):
        self.logger.debug('[{}] finished message: {}'.format(conn, message_id))
        self.backoff.success()
        self.update_ready(conn)
        self.handle_backoff()
        self.on_finish.send(self, message_id=message_id)

    def handle_requeue(self, conn, message_id, timeout):
        msg = '[{}] requeued message: {} ({})'
        self.logger.debug(msg.format(conn, message_id, timeout))
        self.backoff.failure()
        self.update_ready(conn)
        self.handle_backoff()
        self.on_requeue.send(self, message_id=message_id, timeout=timeout)

    def handle_backoff(self):
        if self.state == BACKOFF:
            return

        if self.state == THROTTLED and self.backoff.is_reset():
            return self.complete_backoff()

        if not self.backoff.is_reset():
            return self.start_backoff()
