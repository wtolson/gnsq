# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
import time

import blinker

from gevent import socket

from . import protocol as nsq
from . import errors

from .decorators import cached_property, deprecated
from .httpclient import HTTPClient, USERAGENT
from .message import Message
from .states import CONNECTED, DISCONNECTED, INIT
from .stream import Stream

HOSTNAME = socket.gethostname()
SHORTNAME = HOSTNAME.split('.')[0]


class NsqdTCPClient(object):
    """Low level object representing a TCP connection to nsqd.

    :param address: the host or ip address of the nsqd

    :param port: the nsqd tcp port to connect to

    :param timeout: the timeout for read/write operations (in seconds)

    :param client_id: an identifier used to disambiguate this client (defaults
        to the first part of the hostname)

    :param hostname: the hostname where the client is deployed (defaults to the
        clients hostname)

    :param heartbeat_interval: the amount of time in seconds to negotiate with
        the connected producers to send heartbeats (requires nsqd 0.2.19+)

    :param output_buffer_size: size of the buffer (in bytes) used by nsqd for
        buffering writes to this connection

    :param output_buffer_timeout: timeout (in ms) used by nsqd before flushing
        buffered writes (set to 0 to disable). Warning: configuring clients with
        an extremely low (< 25ms) output_buffer_timeout has a significant effect
        on nsqd CPU usage (particularly with > 50 clients connected).

    :param tls_v1: enable TLS v1 encryption (requires nsqd 0.2.22+)

    :param tls_options: dictionary of options to pass to `ssl.wrap_socket()
        <http://docs.python.org/2/library/ssl.html#ssl.wrap_socket>`_

    :param snappy: enable Snappy stream compression (requires nsqd 0.2.23+)

    :param deflate: enable deflate stream compression (requires nsqd 0.2.23+)

    :param deflate_level: configure the deflate compression level for this
        connection (requires nsqd 0.2.23+)

    :param sample_rate: take only a sample of the messages being sent to the
        client. Not setting this or setting it to 0 will ensure you get all the
        messages destined for the client. Sample rate can be greater than 0 or
        less than 100 and the client will receive that percentage of the message
        traffic. (requires nsqd 0.2.25+)

    :param auth_secret: a string passed when using nsq auth (requires
        nsqd 0.2.29+)

    :param user_agent: a string identifying the agent for this client in the
        spirit of HTTP (default: ``<client_library_name>/<version>``) (requires
        nsqd 0.2.25+)
    """
    def __init__(
        self,
        address='127.0.0.1',
        port=4150,
        timeout=60.0,
        client_id=None,
        hostname=None,
        heartbeat_interval=30,
        output_buffer_size=16 * 1024,
        output_buffer_timeout=250,
        tls_v1=False,
        tls_options=None,
        snappy=False,
        deflate=False,
        deflate_level=6,
        sample_rate=0,
        auth_secret=None,
        user_agent=USERAGENT,
    ):
        self.address = address
        self.port = port
        self.timeout = timeout

        self.client_id = client_id or SHORTNAME
        self.hostname = hostname or HOSTNAME
        self.heartbeat_interval = 1000 * heartbeat_interval
        self.output_buffer_size = output_buffer_size
        self.output_buffer_timeout = output_buffer_timeout
        self.tls_v1 = tls_v1
        self.tls_options = tls_options
        self.snappy = snappy
        self.deflate = deflate
        self.deflate_level = deflate_level
        self.sample_rate = sample_rate
        self.auth_secret = auth_secret
        self.user_agent = user_agent

        self.state = INIT
        self.last_response = time.time()
        self.last_message = time.time()
        self.ready_count = 0
        self.in_flight = 0
        self.max_ready_count = 2500

        self._frame_handlers = {
            nsq.FRAME_TYPE_RESPONSE: self.handle_response,
            nsq.FRAME_TYPE_ERROR: self.handle_error,
            nsq.FRAME_TYPE_MESSAGE: self.handle_message
        }

    @cached_property
    def on_message(self):
        """Emitted when a message frame is received.

        The signal sender is the connection and the ``message`` is sent as an
        argument.
        """
        return blinker.Signal(doc='Emitted when a message frame is received.')

    @cached_property
    def on_response(self):
        """Emitted when a response frame is received.

        The signal sender is the connection and the ``response`` is sent as an
        argument.
        """
        return blinker.Signal(doc='Emitted when a response frame is received.')

    @cached_property
    def on_error(self):
        """Emitted when an error frame is received.

        The signal sender is the connection and the ``error`` is sent as an
        argument.
        """
        return blinker.Signal(doc='Emitted when a error frame is received.')

    @cached_property
    def on_finish(self):
        """Emitted after :meth:`finish`.

        Sent after a message owned by this connection is successfully finished.
        The signal sender is the connection and the ``message_id`` is sent as an
        argument.
        """
        return blinker.Signal(doc='Emitted after the a message is finished.')

    @cached_property
    def on_requeue(self):
        """Emitted after :meth:`requeue`.

        Sent after a message owned by this connection is requeued. The signal
        sender is the connection and the ``message_id``, ``timeout`` and
        ``backoff`` flag are sent as arguments.
        """
        return blinker.Signal(doc='Emitted after the a message is requeued.')

    @cached_property
    def on_auth(self):
        """Emitted after the connection is successfully authenticated.

        The signal sender is the connection and the parsed ``response`` is sent
        as arguments.
        """
        return blinker.Signal(
            doc='Emitted after the connection is successfully authenticated.')

    @cached_property
    def on_close(self):
        """Emitted after :meth:`close_stream`.

        Sent after the connection socket has closed. The signal sender is the
        connection.
        """
        return blinker.Signal(doc='Emitted after the connection is closed.')

    @property
    def is_connected(self):
        """Check if the client is currently connected."""
        return self.state == CONNECTED

    @property
    def is_starved(self):
        """Evaluate whether the connection is starved.

        This property should be used by message handlers to reliably identify
        when to process a batch of messages.
        """
        return self.in_flight >= max(self.ready_count * 0.85, 1)

    def connect(self):
        """Initialize connection to the nsqd."""
        if self.state == DISCONNECTED:
            raise errors.NSQException('connection already closed')

        if self.is_connected:
            return

        stream = Stream(self.address, self.port, self.timeout)
        stream.connect()

        self.stream = stream
        self.state = CONNECTED
        self.send(nsq.MAGIC_V2)

    def close_stream(self):
        """Close the underlying socket."""
        if not self.is_connected:
            return

        self.stream.close()
        self.state = DISCONNECTED
        self.on_close.send(self)

    def send(self, data):
        try:
            return self.stream.send(data)
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
        """Read an individual response from nsqd.

        :returns: tuple of the frame type and the processed data.
        """
        response = self._read_response()
        frame, data = nsq.unpack_response(response)
        self.last_response = time.time()

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
        self.last_message = time.time()
        self.in_flight += 1

        message = Message(*nsq.unpack_message(data))
        message.on_finish.connect(self.handle_finish)
        message.on_requeue.connect(self.handle_requeue)
        message.on_touch.connect(self.handle_touch)

        self.on_message.send(self, message=message)
        return message

    def handle_finish(self, message):
        self.finish(message.id)

    def handle_requeue(self, message, timeout, backoff):
        self.requeue(message.id, timeout, backoff)

    def handle_touch(self, message):
        self.touch(message.id)

    def finish_inflight(self):
        self.in_flight -= 1

    def listen(self):
        """Listen to incoming responses until the connection closes."""
        while self.is_connected:
            self.read_response()

    def check_ok(self, expected=nsq.OK):
        frame, data = self.read_response()
        if frame == nsq.FRAME_TYPE_ERROR:
            raise data

        if frame != nsq.FRAME_TYPE_RESPONSE:
            raise errors.NSQException('expected response frame')

        if data != expected:
            raise errors.NSQException('unexpected response {!r}'.format(data))

    def upgrade_to_tls(self):
        self.stream.upgrade_to_tls(**self.tls_options)
        self.check_ok()

    def upgrade_to_snappy(self):
        self.stream.upgrade_to_snappy()
        self.check_ok()

    def upgrade_to_defalte(self):
        self.stream.upgrade_to_defalte(self.deflate_level)
        self.check_ok()

    def identify(self):
        """Update client metadata on the server and negotiate features.

        :returns: nsqd response data if there was feature negotiation,
            otherwise ``None``
        """
        self.send(nsq.identify({
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

        if data == nsq.OK:
            return

        try:
            data = json.loads(data.decode('utf-8'))

        except ValueError:
            self.close_stream()
            raise errors.NSQException(
                'failed to parse IDENTIFY response JSON from nsqd: '
                '{!r}'.format(data))

        self.max_ready_count = data.get('max_rdy_count', self.max_ready_count)

        if self.tls_v1 and data.get('tls_v1'):
            self.upgrade_to_tls()

        if self.snappy and data.get('snappy'):
            self.upgrade_to_snappy()

        elif self.deflate and data.get('deflate'):
            self.deflate_level = data.get('deflate_level', self.deflate_level)
            self.upgrade_to_defalte()

        if self.auth_secret and data.get('auth_required'):
            self.auth()

        return data

    def auth(self):
        """Send authorization secret to nsqd."""
        self.send(nsq.auth(self.auth_secret))
        frame, data = self.read_response()

        if frame == nsq.FRAME_TYPE_ERROR:
            raise data

        try:
            response = json.loads(data.decode('utf-8'))
        except ValueError:
            self.close_stream()
            raise errors.NSQException(
                'failed to parse AUTH response JSON from nsqd: '
                '{!r}'.format(data))

        self.on_auth.send(self, response=response)
        return response

    def subscribe(self, topic, channel):
        """Subscribe to a nsq `topic` and `channel`."""
        self.send(nsq.subscribe(topic, channel))

    def publish(self, topic, data, defer=None):
        """Publish a message to the given topic over tcp.

        :param topic: the topic to publish to

        :param data: bytestring data to publish

        :param defer: duration in milliseconds to defer before publishing
            (requires nsq 0.3.6)
        """
        if defer is None:
            self.send(nsq.publish(topic, data))
        else:
            self.send(nsq.deferpublish(topic, data, defer))

    def multipublish(self, topic, messages):
        """Publish an iterable of messages to the given topic over http.

        :param topic: the topic to publish to

        :param messages: iterable of bytestrings to publish
        """
        self.send(nsq.multipublish(topic, messages))

    def ready(self, count):
        """Indicate you are ready to receive ``count`` messages."""
        self.ready_count = count
        self.send(nsq.ready(count))

    def finish(self, message_id):
        """Finish a message (indicate successful processing)."""
        self.send(nsq.finish(message_id))
        self.finish_inflight()
        self.on_finish.send(self, message_id=message_id)

    def requeue(self, message_id, timeout=0, backoff=True):
        """Re-queue a message (indicate failure to process)."""
        self.send(nsq.requeue(message_id, timeout))
        self.finish_inflight()
        self.on_requeue.send(
            self,
            message_id=message_id,
            timeout=timeout,
            backoff=backoff
        )

    def touch(self, message_id):
        """Reset the timeout for an in-flight message."""
        self.send(nsq.touch(message_id))

    def close(self):
        """Indicate no more messages should be sent."""
        self.send(nsq.close())

    def nop(self):
        """Send no-op to nsqd. Used to keep connection alive."""
        self.send(nsq.nop())

    def __str__(self):
        return '{}:{}'.format(self.address, self.port)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return isinstance(other, type(self)) and str(self) == str(other)

    def __cmp__(self, other):
        return hash(self) - hash(other)

    def __lt__(self, other):
        return hash(self) < hash(other)


class NsqdHTTPClient(HTTPClient):
    """Low level http client for nsqd.

    :param host: nsqd host address (default: localhost)

    :param port: nsqd http port (default: 4151)

    :param useragent: useragent sent to nsqd (default:
        ``<client_library_name>/<version>``)

    :param connection_class: override the http connection class
    """
    def __init__(self, host='localhost', port=4151, **kwargs):
        super(NsqdHTTPClient, self).__init__(host, port, **kwargs)

    def publish(self, topic, data, defer=None):
        """Publish a message to the given topic over http.

        :param topic: the topic to publish to

        :param data: bytestring data to publish

        :param defer: duration in millisconds to defer before publishing
            (requires nsq 0.3.6)
        """
        nsq.assert_valid_topic_name(topic)
        fields = {'topic': topic}

        if defer is not None:
            fields['defer'] = '{}'.format(defer)

        return self._request('POST', '/pub', fields=fields, body=data)

    def _validate_mpub_message(self, message):
        if b'\n' not in message:
            return message
        raise errors.NSQException(
            'newlines are not allowed in http multipublish')

    def multipublish(self, topic, messages, binary=False):
        """Publish an iterable of messages to the given topic over http.

        :param topic: the topic to publish to

        :param messages: iterable of bytestrings to publish

        :param binary: enable binary mode. defaults to False
            (requires nsq 1.0.0)

        By default multipublish expects messages to be delimited by ``"\\n"``,
        use the binary flag to enable binary mode where the POST body is
        expected to be in the following wire protocol format.
        """
        nsq.assert_valid_topic_name(topic)
        fields = {'topic': topic}

        if binary:
            fields['binary'] = 'true'
            body = nsq.multipublish_body(messages)
        else:
            body = b'\n'.join(self._validate_mpub_message(m) for m in messages)

        return self._request('POST', '/mpub', fields=fields, body=body)

    def create_topic(self, topic):
        """Create a topic."""
        nsq.assert_valid_topic_name(topic)
        return self._request('POST', '/topic/create', fields={'topic': topic})

    def delete_topic(self, topic):
        """Delete a topic."""
        nsq.assert_valid_topic_name(topic)
        return self._request('POST', '/topic/delete', fields={'topic': topic})

    def create_channel(self, topic, channel):
        """Create a channel for an existing topic."""
        nsq.assert_valid_topic_name(topic)
        nsq.assert_valid_channel_name(channel)
        return self._request('POST', '/channel/create',
                             fields={'topic': topic, 'channel': channel})

    def delete_channel(self, topic, channel):
        """Delete an existing channel for an existing topic."""
        nsq.assert_valid_topic_name(topic)
        nsq.assert_valid_channel_name(channel)
        return self._request('POST', '/channel/delete',
                             fields={'topic': topic, 'channel': channel})

    def empty_topic(self, topic):
        """Empty all the queued messages for an existing topic."""
        nsq.assert_valid_topic_name(topic)
        return self._request('POST', '/topic/empty', fields={'topic': topic})

    def empty_channel(self, topic, channel):
        """Empty all the queued messages for an existing channel."""
        nsq.assert_valid_topic_name(topic)
        nsq.assert_valid_channel_name(channel)
        return self._request('POST', '/channel/empty',
                             fields={'topic': topic, 'channel': channel})

    def pause_topic(self, topic):
        """Pause message flow to all channels on an existing topic.

        Messages will queue at topic.
        """
        nsq.assert_valid_topic_name(topic)
        return self._request('POST', '/topic/pause', fields={'topic': topic})

    def unpause_topic(self, topic):
        """Resume message flow to channels of an existing, paused, topic."""
        nsq.assert_valid_topic_name(topic)
        return self._request('POST', '/topic/unpause', fields={'topic': topic})

    def pause_channel(self, topic, channel):
        """Pause message flow to consumers of an existing channel.

        Messages will queue at channel.
        """
        nsq.assert_valid_topic_name(topic)
        nsq.assert_valid_channel_name(channel)
        return self._request('POST', '/channel/pause',
                             fields={'topic': topic, 'channel': channel})

    def unpause_channel(self, topic, channel):
        """Resume message flow to consumers of an existing, paused, channel."""
        nsq.assert_valid_topic_name(topic)
        nsq.assert_valid_channel_name(channel)
        return self._request('POST', '/channel/unpause',
                             fields={'topic': topic, 'channel': channel})

    def stats(self, topic=None, channel=None, text=False):
        """Return internal instrumented statistics.

        :param topic: (optional) filter to topic

        :param channel: (optional) filter to channel

        :param text: return the stats as a string (default: ``False``)
        """
        if text:
            fields = {'format': 'text'}
        else:
            fields = {'format': 'json'}

        if topic:
            nsq.assert_valid_topic_name(topic)
            fields['topic'] = topic

        if channel:
            nsq.assert_valid_channel_name(channel)
            fields['channel'] = channel

        return self._request('GET', '/stats', fields=fields)

    def ping(self):
        """Monitoring endpoint.

        :returns: should return ``"OK"``, otherwise raises an exception.
        """
        return self._request('GET', '/ping')

    def info(self):
        """Returns version information."""
        return self._request('GET', '/info')


class Nsqd(object):
    """Use :class:`NsqdTCPClient` or :class:`NsqdHTTPClient` instead.

    .. deprecated:: 1.0.0
    """
    @deprecated
    def __init__(self, address='127.0.0.1', tcp_port=4150, http_port=4151,
                 **kwargs):
        """Use :class:`NsqdTCPClient` or :class:`NsqdHTTPClient` instead.

        .. deprecated:: 1.0.0
        """
        self.address = address
        self.tcp_port = tcp_port
        self.http_port = http_port

        self.__tcp_client = NsqdTCPClient(address, tcp_port, **kwargs)
        self.__http_client = NsqdHTTPClient(address, http_port)

    @property
    def base_url(self):
        return 'http://{}:{}/'.format(self.address, self.http_port)

    @deprecated
    def publish_tcp(self, topic, data, **kwargs):
        """Use :meth:`NsqdTCPClient.publish` instead.

        .. deprecated:: 1.0.0
        """
        return self.__tcp_client.publish(topic, data, **kwargs)

    @deprecated
    def publish_http(self, topic, data, **kwargs):
        """Use :meth:`NsqdHTTPClient.publish` instead.

        .. deprecated:: 1.0.0
        """
        self.__http_client.publish(topic, data, **kwargs)

    def publish(self, topic, data, *args, **kwargs):
        if self.__tcp_client.is_connected:
            return self.__tcp_client.publish(topic, data, *args, **kwargs)
        else:
            return self.__http_client.publish(topic, data, *args, **kwargs)

    @deprecated
    def multipublish_tcp(self, topic, messages, **kwargs):
        """Use :meth:`NsqdTCPClient.multipublish` instead.

        .. deprecated:: 1.0.0
        """
        return self.__tcp_client.multipublish(topic, messages, **kwargs)

    @deprecated
    def multipublish_http(self, topic, messages, **kwargs):
        """Use :meth:`NsqdHTTPClient.multipublish` instead.

        .. deprecated:: 1.0.0
        """
        return self.__http_client.multipublish(topic, messages, **kwargs)

    def multipublish(self, topic, messages, *args, **kwargs):
        if self.__tcp_client.is_connected:
            return self.__tcp_client.multipublish(
                topic, messages, *args, **kwargs)
        else:
            return self.__http_client.multipublish(
                topic, messages, *args, **kwargs)

    def __getattr__(self, name):
        for client in (self.__tcp_client, self.__http_client):
            try:
                return getattr(client, name)
            except AttributeError:
                pass

        return super(Nsqd, self).__getattr__(name)

    def __str__(self):
        return '{}:{}'.format(self.address, self.tcp_port)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return isinstance(other, type(self)) and str(self) == str(other)

    def __cmp__(self, other):
        return hash(self) - hash(other)

    def __lt__(self, other):
        return hash(self) < hash(other)
