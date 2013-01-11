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
        logger                 = None
    ):
        self.nsqd_tcp_addresses = assert_list(nsqd_tcp_addresses)
        self.lookupd            = Lookupd(lookupd_http_addresses)
        assert self.nsqd_tcp_addresses or self.lookupd.addresses

        self.topic                 = topic
        self.channel               = channel
        self.async                 = async
        self.max_tries             = max_tries
        self.max_in_flight         = max_in_flight
        self.lookupd_poll_interval = lookupd_poll_interval
        self.logger                = logger or logging.getLogger(__name__)

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
        self._poll()

    def connection_max_in_flight(self):
        return max(1, self.max_in_flight / max(1, len(self.conns)))

    def query_nsqd(self):
        self.logger.debug('Querying nsqd...')
        for address in self.nsqd_tcp_addresses:
            address, port = address.split(':')
            self.connect_to_nsqd(address, int(port))

    def query_lookupd(self):
        self.logger.debug('Querying lookupd...')
        for producer in self.lookupd.iter_lookup(self.topic):
            self.connect_to_nsqd(
                producer['address'],
                producer['tcp_port'],
                producer['http_port']
            )

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
        stats = conn.stats()
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
        return random.sample(self.conns, 1)[0]

    def publish(self, topic, message):
        conn = self.random_connection()
        if conn is None:
            raise NSQNoConnections()

        conn.publish(topic, message)

    def connect_to_nsqd(self, address, tcp_port, http_port=None):
        assert isinstance(address, (str, unicode))
        assert isinstance(tcp_port, int)
        assert isinstance(http_port, int) or http_port is None

        self.logger.debug('Connecting to %s:%s...' % (address, tcp_port))
        conn = Nsqd(address, tcp_port, http_port)

        if conn in self.conns:
            self.logger.debug('Already connected.')
            return

        conn.on_response.connect(self.handle_response)
        conn.on_error.connect(self.handle_error)
        conn.on_message.connect(self.handle_message)
        conn.on_finish.connect(self.handle_finish)
        conn.on_requeue.connect(self.handle_requeue)

        try:
            conn.connect()
            conn.subscribe(self.topic, self.channel)
            conn.ready(self.connection_max_in_flight())
        except NSQException:
            self.logger.warn('Failed connecting to %s:%s' % (address, tcp_port))
            return

        self.logger.info('Connected to %s:%s' % (address, tcp_port))
        self.conns.add(conn)
        conn.worker = gevent.spawn(self._listen, conn)

    def _listen(self, conn):
        try:
            conn.listen()
        except NSQException:
            self.logger.info('Lost connection to %s:%s' % (conn.address, conn.tcp_port))

        self.conns.remove(conn)
        conn.kill()

    def handle_response(self, conn, response):
        self.on_response.send(self, conn=conn, response=response)

    def handle_error(self, conn, error):
        self.on_error.send(self, conn=conn, error=error)

    def handle_message(self, conn, message):
        try:
            self.on_message.send(self, conn=conn, message=message)
            if not self.async:
                self.finish(message)
            return

        except NSQRequeueMessage:
            pass

        except Exception:
            logging.exception('[%s] caught exception while handling message' % conn)

        if message.attempts > self.max_tries:
            logging.warning("giving up on message '%s' after max tries %d", message.id, self.max_tries)
            return message.finish()

        message.requeue()

    def update_ready(self, conn):
        max_in_flight = self.connection_max_in_flight()
        if conn.ready_count < (0.25 * max_in_flight):
            conn.ready(max_in_flight)

    def handle_finish(self, conn, message_id):
        self.on_finish.send(self, conn=conn, message_id=message_id)
        self.update_ready(conn)

    def handle_requeue(self, conn, message_id, timeout):
        self.on_requeue.send(self, conn=conn, message_id=message_id, timeout=timeout)
        self.update_ready(conn)

    def close(self):
        for conn in self.conns:
            conn.close()

    def join(self):
        gevent.joinall([conn.worker for conn in self.conns])
