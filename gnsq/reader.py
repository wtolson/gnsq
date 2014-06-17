import logging
import random
import gevent
import blinker

from itertools import cycle
from collections import defaultdict

from .lookupd import Lookupd
from .nsqd import Nsqd
from .backofftimer import BackoffTimer

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
        async=False,
        max_tries=5,
        max_in_flight=1,
        requeue_delay=0,
        lookupd_poll_interval=60,
        lookupd_poll_jitter=0.3,
        low_rdy_idle_timeout=10,
        max_backoff_duration=128,
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
        self.max_backoff_duration = max_backoff_duration

        self.logger = logging.getLogger(__name__)
        self.backofftimers = defaultdict(self.create_backoff)

        self.on_response = blinker.Signal()
        self.on_error = blinker.Signal()
        self.on_message = blinker.Signal()
        self.on_finish = blinker.Signal()
        self.on_requeue = blinker.Signal()
        self.on_exception = blinker.Signal()

        self.conns = set()
        self.pending = set()

    def start(self):
        self.query_nsqd()
        self.query_lookupd()
        self._poll()  # spawn poll only if we have lookupds
        # TODO: run _redistribute_rdy_state

    def connection_max_in_flight(self):
        return max(1, self.max_in_flight / max(1, len(self.conns)))

    def is_starved(self):
        for conn in self.conns:
            if conn.in_flight > 0 and conn.in_flight >= (conn.last_rdy * 0.85):
                return True
        return False

    def query_nsqd(self):
        self.logger.debug('querying nsqd...')
        for address in self.nsqd_tcp_addresses:
            address, port = address.split(':')
            conn = Nsqd(address, int(port))
            self.connect_to_nsqd(conn)

    def query_lookupd(self):
        if not self.lookupds:
            return

        self.logger.debug('querying lookupd...')
        lookupd = self.iterlookupds.next()

        try:
            producers = lookupd.lookup(self.topic)['producers']

        except Exception as error:
            msg = 'Failed to lookup {} on {} ({})'
            self.logger.warn(msg.format(self.topic, lookupd.address, error))
            return

        for producer in producers:
            conn = Nsqd(
                producer.get('broadcast_address') or producer['address'],
                producer['tcp_port'],
                producer['http_port']
            )
            self.connect_to_nsqd(conn)

    def create_backoff(self):
        return BackoffTimer(max_interval=self.max_backoff_duration)

    def _poll(self):
        delay = self.lookupd_poll_interval * self.lookupd_poll_jitter
        gevent.sleep(random.random() * delay)

        while 1:
            gevent.sleep(self.lookupd_poll_interval)
            self.query_nsqd()
            self.query_lookupd()

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

            if conn.max_rdy_count < self.max_in_flight:
                self.logger.warning(' '.join([
                    '[{}] max RDY count {} < reader max in flight {},',
                    'truncation possible'
                ]).format(conn, conn.max_rdy_count, self.max_in_flight))

            conn.subscribe(self.topic, self.channel)

            # Send RDY 1 if we're the first connection
            if not self.conns:
                conn.ready(1)

        except NSQException as error:
            msg = '[{}] connection failed ({!r})'.format(conn, error)
            self.logger.debug(msg)
            self.handle_connection_failure(conn)
            return

        finally:
            self.pending.remove(conn)

        self.conns.add(conn)
        conn.worker = gevent.spawn(self._listen, conn)

        self.logger.info('[{}] connection successful'.format(conn))
        self.handle_connection_success(conn)

    def _listen(self, conn):
        try:
            conn.listen()
        except NSQException as error:
            msg = '[{}] connection lost ({!r})'.format(conn, error)
            self.logger.warning(msg)

        self.handle_connection_failure(conn)

    def handle_connection_failure(self, conn):
        self.conns.discard(conn)
        conn.close_stream()

        if str(conn) not in self.nsqd_tcp_addresses:
            return

        seconds = self.backofftimers[str(conn)].failure().get_interval()
        self.logger.debug('[{}] retrying in {}s'.format(conn, seconds))
        gevent.spawn_later(seconds, self.connect_to_nsqd, conn)

    def handle_connection_success(self, conn):
        if str(conn) not in self.nsqd_tcp_addresses:
            return
        self.backofftimers[str(conn)].success()

    def handle_response(self, conn, response):
        self.logger.debug('[{}] response: {}'.format(conn, response))
        self.on_response.send(self, conn=conn, response=response)

    def handle_error(self, conn, error):
        self.logger.debug('[{}] error: {}'.format(conn, error))
        self.on_error.send(self, conn=conn, error=error)

    def handle_message(self, conn, message):
        self.logger.debug('[{}] got message: {}'.format(conn, message.id))

        if self.max_tries and message.attempts > self.max_tries:
            msg = "giving up on message '{}' after max tries {}"
            self.logger.warning(msg.format(message.id, self.max_tries))
            return message.finish()

        try:
            self.on_message.send(self, conn=conn, message=message)
            if not self.async:
                message.finish()
            return

        except NSQRequeueMessage:
            pass

        except Exception as error:
            msg = '[{}] caught exception while handling message'.format(conn)
            self.logger.exception(msg)
            self.on_exception(self, conn=conn, message=message, error=error)

        message.requeue(self.requeue_delay)

    def update_ready(self, conn):
        max_in_flight = self.connection_max_in_flight()
        if conn.ready_count < (0.25 * max_in_flight):
            conn.ready(max_in_flight)

    def handle_finish(self, conn, message_id):
        self.logger.debug('[{}] finished message: {}'.format(conn, message_id))
        self.on_finish.send(self, conn=conn, message_id=message_id)
        self.update_ready(conn)

    def handle_requeue(self, conn, message_id, timeout):
        msg = '[{}] requeued message: {} ({})'
        self.logger.debug(msg.format(conn, message_id, timeout))
        self.on_requeue.send(
            self,
            conn=conn,
            message_id=message_id,
            timeout=timeout
        )
        self.update_ready(conn)

    def close(self):
        for conn in self.conns:
            conn.close()

    def join(self, timeout=None, raise_error=False):
        # FIXME
        workers = [c._send_worker for c in self.conns if c._send_worker]
        gevent.joinall(workers, timeout, raise_error)
