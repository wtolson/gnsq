import logging
import random
import gevent
import blinker

from .lookupd import Lookupd
from .nsqd    import Nsqd
from .util    import assert_list

from .errors import (
    NSQException,
    NSQNoConnections,
    NSQRequeueMessage
)


class Reader(object):
    def __init__(self,
        topic,
        channel,
        nsqd_tcp_addresses     = [],
        lookupd_http_addresses = [],
        async                  = False,
        max_tries              = 5,
        max_in_flight          = 1,
        lookupd_poll_interval  = 120,
        requeue_delay          = 0
    ):
        lookupd_http_addresses  = assert_list(lookupd_http_addresses)
        self.lookupds           = [Lookupd(a) for a in lookupd_http_addresses]
        self.nsqd_tcp_addresses = assert_list(nsqd_tcp_addresses)
        assert self.nsqd_tcp_addresses or self.lookupds

        self.topic                 = topic
        self.channel               = channel
        self.async                 = async
        self.max_tries             = max_tries
        self.max_in_flight         = max_in_flight
        self.lookupd_poll_interval = lookupd_poll_interval
        self.requeue_delay         = requeue_delay
        self.logger                = logging.getLogger(__name__)

        self.on_response = blinker.Signal()
        self.on_error    = blinker.Signal()
        self.on_message  = blinker.Signal()
        self.on_finish   = blinker.Signal()
        self.on_requeue  = blinker.Signal()

        self.conns = set()
        self.stats = {}

    def start(self):
        self.query_nsqd()
        self.query_lookupd()
        self.update_stats()
        self._poll()

    def connection_max_in_flight(self):
        return max(1, self.max_in_flight / max(1, len(self.conns)))

    def query_nsqd(self):
        self.logger.debug('querying nsqd...')
        for address in self.nsqd_tcp_addresses:
            address, port = address.split(':')
            self.connect_to_nsqd(address, int(port))

    def _query_lookupd(self, lookupd):
        try:
            producers = lookupd.lookup(self.topic)

        except Exception as error:
            template = 'Failed to lookup %s on %s (%s)'
            data = (self.topic, lookupd.address, error)
            self.logger.warn(template % data)
            return

        for producer in producers:
            self.connect_to_nsqd(
                producer['address'],
                producer['tcp_port'],
                producer['http_port']
            )

    def query_lookupd(self):
        self.logger.debug('querying lookupd...')
        for lookupd in self.lookupds:
            self._query_lookupd(lookupd)

    def _poll(self):
        gevent.sleep(random.random() * self.lookupd_poll_interval * 0.1)
        while 1:
            gevent.sleep(self.lookupd_poll_interval)
            self.query_nsqd()
            self.query_lookupd()
            self.update_stats()

    def update_stats(self):
        stats = {}
        for conn in self.conns:
            stats[conn] = self.get_stats(conn)

        self.stats = stats

    def get_stats(self, conn):
        try:
            stats = conn.stats()
        except Exception as error:
            self.logger.warn('[%s] stats lookup failed (%r)' % (conn, error))
            return None

        if stats is None:
            return None

        for topic in stats['topics']:
            if topic['topic_name'] != self.topic:
                continue

            for channel in topic['channels']:
                if channel['channel_name'] != self.channel:
                    continue

                return channel

        return None

    def smallest_depth(self):
        if len(conn) == 0:
            return None

        stats  = self.stats
        depths = [(stats.get(c, {}).get('depth'), c) for c in self.conns]

        return max(depths)[1]

    def random_connection(self):
        if not self.conns:
            return None
        return random.choice(self.conns)

    def publish(self, topic, message):
        conn = self.random_connection()
        if conn is None:
            raise NSQNoConnections()

        conn.publish(topic, message)

    def connect_to_nsqd(self, address, tcp_port, http_port=None):
        assert isinstance(address, (str, unicode))
        assert isinstance(tcp_port, int)
        assert isinstance(http_port, int) or http_port is None

        conn = Nsqd(address, tcp_port, http_port)
        if conn in self.conns:
            self.logger.debug('[%s] already connected' % conn)
            return

        self.logger.debug('[%s] connecting...' % conn)

        conn.on_response.connect(self.handle_response)
        conn.on_error.connect(self.handle_error)
        conn.on_message.connect(self.handle_message)
        conn.on_finish.connect(self.handle_finish)
        conn.on_requeue.connect(self.handle_requeue)

        try:
            conn.connect()
            conn.identify()
            conn.subscribe(self.topic, self.channel)
            conn.ready(self.connection_max_in_flight())
        except NSQException as error:
            self.logger.debug('[%s] connection failed (%r)' % (conn, error))
            return

        self.logger.info('[%s] connection successful' % conn)
        self.conns.add(conn)
        conn.worker = gevent.spawn(self._listen, conn)

    def _listen(self, conn):
        try:
            conn.listen()
        except NSQException as error:
            self.logger.warning('[%s] connection lost (%r)' % (conn, error))

        self.conns.remove(conn)
        conn.kill()

    def handle_response(self, conn, response):
        self.logger.debug('[%s] response: %s' % (conn, response))
        self.on_response.send(self, conn=conn, response=response)

    def handle_error(self, conn, error):
        self.logger.debug('[%s] error: %s' % (conn, error))
        self.on_error.send(self, conn=conn, error=error)

    def handle_message(self, conn, message):
        self.logger.debug('[%s] got message: %s' % (conn, message.id))

        if self.max_tries and message.attempts > self.max_tries:
            template = "giving up on message '%s' after max tries %d"
            self.logger.warning(template, message.id, self.max_tries)
            return message.finish()

        try:
            self.on_message.send(self, conn=conn, message=message)
            if not self.async:
                self.finish(message)
            return

        except NSQRequeueMessage:
            pass

        except Exception:
            template = '[%s] caught exception while handling message'
            self.logger.exception(template % conn)

        message.requeue(self.requeue_delay)

    def update_ready(self, conn):
        max_in_flight = self.connection_max_in_flight()
        if conn.ready_count < (0.25 * max_in_flight):
            conn.ready(max_in_flight)

    def handle_finish(self, conn, message_id):
        self.logger.debug('[%s] finished message: %s' % (conn, message_id))
        self.on_finish.send(self, conn=conn, message_id=message_id)
        self.update_ready(conn)

    def handle_requeue(self, conn, message_id, timeout):
        template = '[%s] requeued message: %s (%s)'
        self.logger.debug(template % (conn, message_id, timeout))
        self.on_requeue.send(
            self,
            conn       = conn,
            message_id = message_id,
            timeout    = timeout
        )
        self.update_ready(conn)

    def close(self):
        for conn in self.conns:
            conn.close()

    def join(self):
        gevent.joinall([conn.worker for conn in self.conns])
