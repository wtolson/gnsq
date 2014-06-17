import blinker
from gevent import socket

try:
    import simplejson as json
except ImportError:
    import json  # pyflakes.ignore

from . import protocal as nsq
from . import errors

from .message import Message
from .httpclient import HTTPClient
from .states import INIT, CONNECTED, DISCONNECTED
from .stream import Stream
from .version import __version__

HOSTNAME = socket.gethostname()
SHORTNAME = HOSTNAME.split('.')[0]
USERAGENT = 'gnsq/{}'.format(__version__)


class Nsqd(HTTPClient):
    def __init__(
        self,
        address='127.0.0.1',
        tcp_port=4150,
        http_port=4151,
        timeout=60.0,
        client_id=SHORTNAME,
        hostname=HOSTNAME,
        heartbeat_interval=30,
        output_buffer_size=16 * 1024,
        output_buffer_timeout=250,
        tls_v1=False,
        tls_options=None,
        snappy=False,
        deflate=False,
        deflate_level=6,
        sample_rate=0,
        user_agent=USERAGENT,
    ):
        self.address = address
        self.tcp_port = tcp_port
        self.http_port = http_port
        self.timeout = timeout

        self.client_id = client_id
        self.hostname = hostname
        self.heartbeat_interval = 1000 * heartbeat_interval
        self.output_buffer_size = output_buffer_size
        self.output_buffer_timeout = output_buffer_timeout
        self.tls_v1 = tls_v1
        self.tls_options = tls_options
        self.snappy = snappy
        self.deflate = deflate
        self.deflate_level = deflate_level
        self.sample_rate = sample_rate
        self.user_agent = user_agent

        self.state = INIT
        self.last_ready = 0
        self.ready_count = 0
        self.in_flight = 0

        self.on_response = blinker.Signal()
        self.on_error = blinker.Signal()
        self.on_message = blinker.Signal()
        self.on_finish = blinker.Signal()
        self.on_requeue = blinker.Signal()
        self.on_close = blinker.Signal()

        self._frame_handlers = {
            nsq.FRAME_TYPE_RESPONSE: self.handle_response,
            nsq.FRAME_TYPE_ERROR: self.handle_error,
            nsq.FRAME_TYPE_MESSAGE: self.handle_message
        }

    @property
    def connected(self):
        return self.state == CONNECTED

    def connect(self):
        if self.state not in (INIT, DISCONNECTED):
            return

        stream = Stream(self.address, self.tcp_port, self.timeout)
        stream.connect()

        self.stream = stream
        self.state = CONNECTED
        self.send(nsq.MAGIC_V2)

    def close_stream(self):
        self.stream.close()
        self.state = DISCONNECTED
        self.on_close.send(self)

    def send(self, data, async=False):
        try:
            return self.stream.send(data, async)
        except Exception:
            self.close_stream()
            raise

    def _read_response(self):
        try:
            size = nsq.unpack_size(self.stream.read(4))
            return self.stream.read(size)
        except Exception:
            self.close_stream()
            raise

    def read_response(self):
        response = self._read_response()
        frame, data = nsq.unpack_response(response)

        if frame not in self._frame_handlers:
            raise errors.NSQFrameError('unknown frame {}'.format(frame))

        frame_handler = self._frame_handlers[frame]
        processed_data = frame_handler(data)

        return frame, processed_data

    def handle_response(self, data):
        if data == nsq.HEARTBEAT:
            self.nop()

        self.on_response.send(self, response=data)
        return data

    def handle_error(self, data):
        error = errors.make_error(data)
        self.on_error.send(self, error=error)

        if error.fatal:
            self.close_stream()

        return error

    def handle_message(self, data):
        self.ready_count -= 1
        self.in_flight += 1
        message = Message(self, *nsq.unpack_message(data))
        self.on_message.send(self, message=message)
        return message

    def finish_inflight(self):
        self.in_flight -= 1

    def listen(self):
        while self.connected:
            self.read_response()

    def upgrade_to_tls(self):
        self.stream.upgrade_to_tls(**self.tls_options)

    def upgrade_to_snappy(self):
        self.stream.upgrade_to_snappy()

    def upgrade_to_defalte(self):
        self.stream.upgrade_to_defalte(self.deflate_level)

    def identify(self):
        self.send(nsq.identify({
            # nsqd <0.2.28
            'short_id': self.client_id,
            'long_id': self.hostname,

            # nsqd 0.2.28+
            'client_id': self.client_id,
            'hostname': self.hostname,

            # nsqd 0.2.19+
            'feature_negotiation': True,
            'heartbeat_interval': self.heartbeat_interval,

            # nsqd 0.2.21+
            'output_buffer_size': self.output_buffer_size,
            'output_buffer_timeout': self.output_buffer_timeout,

            # nsqd 0.2.22+
            'tls_v1': self.tls_v1,

            # nsqd 0.2.23+
            'snappy': self.snappy,
            'deflate': self.deflate,
            'deflate_level': self.deflate_level,

            # nsqd nsqd 0.2.25+
            'sample_rate': self.sample_rate,
            'user_agent': self.user_agent,
        }))

        frame, data = self.read_response()

        if frame == nsq.FRAME_TYPE_ERROR:
            raise data

        if data == 'OK':
            return

        try:
            data = json.loads(data)

        except ValueError:
            self.close_stream()
            msg = 'failed to parse IDENTIFY response JSON from nsqd: {!r}'
            raise errors.NSQException(msg.format(data))

        if self.tls_v1 and data.get('tls_v1'):
            self.upgrade_to_tls()

        elif self.snappy and data.get('snappy'):
            self.upgrade_to_snappy()

        elif self.deflate and data.get('deflate'):
            self.deflate_level = data.get('deflate_level', self.deflate_level)
            self.upgrade_to_defalte()

        return data

    def subscribe(self, topic, channel):
        self.send(nsq.subscribe(topic, channel))

    def publish_tcp(self, topic, data):
        self.send(nsq.publish(topic, data))

    def multipublish_tcp(self, topic, messages):
        self.send(nsq.multipublish(topic, messages))

    def ready(self, count):
        self.last_ready = count
        self.ready_count = count
        self.send(nsq.ready(count))

    def finish(self, message_id):
        self.send(nsq.finish(message_id))
        self.finish_inflight()
        self.on_finish.send(self, message_id=message_id)

    def requeue(self, message_id, timeout=0):
        self.send(nsq.requeue(message_id, timeout))
        self.finish_inflight()
        self.on_requeue.send(self, message_id=message_id, timeout=timeout)

    def touch(self, message_id):
        self.send(nsq.touch(message_id))

    def close(self):
        self.send(nsq.close())

    def nop(self):
        self.send(nsq.nop())

    @property
    def base_url(self):
        return 'http://{}:{}/'.format(self.address, self.http_port)

    def _check_connection(self):
        if self.http_port:
            return
        raise errors.NSQException(-1, 'no http port')

    def publish_http(self, topic, data):
        nsq.assert_valid_topic_name(topic)
        return self._check_api(
            self.url('put'),
            params={'topic': topic},
            data=data
        )

    def multipublish_http(self, topic, messages):
        nsq.assert_valid_topic_name(topic)

        for message in messages:
            if '\n' not in message:
                continue

            error = 'newlines are not allowed in http multipublish'
            raise errors.NSQException(-1, error)

        return self._check_api(
            self.url('mput'),
            params={'topic': topic},
            data='\n'.join(messages)
        )

    def create_topic(self, topic):
        nsq.assert_valid_topic_name(topic)
        return self._json_api(
            self.url('create_topic'),
            params={'topic': topic}
        )

    def delete_topic(self, topic):
        nsq.assert_valid_topic_name(topic)
        return self._json_api(
            self.url('delete_topic'),
            params={'topic': topic}
        )

    def create_channel(self, topic, channel):
        nsq.assert_valid_topic_name(topic)
        nsq.assert_valid_channel_name(channel)
        return self._json_api(
            self.url('create_channel'),
            params={'topic': topic, 'channel': channel}
        )

    def delete_channel(self, topic, channel):
        nsq.assert_valid_topic_name(topic)
        nsq.assert_valid_channel_name(channel)
        return self._json_api(
            self.url('delete_channel'),
            params={'topic': topic, 'channel': channel}
        )

    def empty_topic(self, topic):
        nsq.assert_valid_topic_name(topic)
        return self._json_api(
            self.url('empty_topic'),
            params={'topic': topic}
        )

    def empty_channel(self, topic, channel):
        nsq.assert_valid_topic_name(topic)
        nsq.assert_valid_channel_name(channel)
        return self._json_api(
            self.url('empty_channel'),
            params={'topic': topic, 'channel': channel}
        )

    def pause_channel(self, topic, channel):
        nsq.assert_valid_topic_name(topic)
        nsq.assert_valid_channel_name(channel)
        return self._json_api(
            self.url('pause_channel'),
            params={'topic': topic, 'channel': channel}
        )

    def unpause_channel(self, topic, channel):
        nsq.assert_valid_topic_name(topic)
        nsq.assert_valid_channel_name(channel)
        return self._json_api(
            self.url('unpause_channel'),
            params={'topic': topic, 'channel': channel}
        )

    def stats(self):
        return self._json_api(self.url('stats'), params={'format': 'json'})

    def ping(self):
        return self._check_api(self.url('ping'))

    def info(self):
        return self._json_api(self.url('info'))

    def publish(self, topic, data):
        if self.connected:
            return self.publish_tcp(topic, data)
        else:
            return self.publish_http(topic, data)

    def multipublish(self, topic, messages):
        if self.connected:
            return self.multipublish_tcp(topic, messages)
        else:
            return self.multipublish_http(topic, messages)

    def __str__(self):
        return self.address + ':' + str(self.tcp_port)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return isinstance(other, Nsqd) and str(self) == str(other)

    def __cmp__(self, other):
        return hash(self) - hash(other)
