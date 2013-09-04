import re
import requests
import blinker

import gevent
from gevent import socket
from gevent.queue import Queue
from gevent.event import AsyncResult
from Queue import Empty

from . import protocal as nsq
from . import errors

from .message import Message

HOSTNAME  = socket.gethostname()
SHORTNAME = HOSTNAME.split('.')[0]

class Nsqd(object):
    def __init__(self,
        address   = '127.0.0.1',
        tcp_port  = 4150,
        http_port = 4151,
        timeout   = 60.0
    ):
        self.address   = address
        self.tcp_port  = tcp_port
        self.http_port = http_port
        self.timeout   = timeout

        self.on_response = blinker.Signal()
        self.on_error    = blinker.Signal()
        self.on_message  = blinker.Signal()
        self.on_finish   = blinker.Signal()
        self.on_requeue  = blinker.Signal()

        self._session     = None
        self._send_worker = None
        self._send_queue  = Queue()

        self._frame_handlers = {
            nsq.FRAME_TYPE_RESPONSE: self.handle_response,
            nsq.FRAME_TYPE_ERROR:    self.handle_error,
            nsq.FRAME_TYPE_MESSAGE:  self.handle_message
        }

        self.reset()

    @property
    def is_connected(self):
        return self._socket is not None

    def connect(self):
        if self.is_connected:
            raise NSQException('already connected')

        self.reset()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(self.timeout)

        try:
            s.connect((self.address, self.tcp_port))
        except socket.error as error:
            raise errors.NSQSocketError(*error)

        self._socket = s
        self._send_worker = gevent.spawn(self._send)
        self.send(nsq.MAGIC_V2)

    def _empty_send_queue(self):
        while 1:
            try:
                data, result = self._send_queue.get_nowait()
            except Empty:
                return

            result.set_exception(errors.NSQException(-1, 'not connected'))

    def kill(self):
        self._socket = None

        if self._send_worker:
            worker, self._send_worker = self._send_worker, None
            worker.kill()

        self._empty_send_queue()

    def send(self, data, async=False):
        if not self.is_connected:
            raise errors.NSQException(-1, 'not connected')

        result = AsyncResult()
        self._send_queue.put((data, result))

        if async:
            return result

        result.get()

    def _send(self):
        while 1:
            data, result = self._send_queue.get()
            if not self.is_connected:
                result.set_exception(errors.NSQException(-1, 'not connected'))
                break

            try:
                self._socket.send(data)
                result.set(True)

            except socket.error as error:
                result.set_exception(errors.NSQSocketError(*error))

            except Exception as error:
                result.set_exception(error)

        self._empty_send_queue()

    def reset(self):
        self.ready_count       = 0
        self.in_flight         = 0
        self._buffer           = ''
        self._socket           = None
        self._on_next_response = None

    def _readn(self, size):
        while len(self._buffer) < size:
            if not self.is_connected:
                raise errors.NSQException(-1, 'not connected')

            try:
                packet = self._socket.recv(4096)
            except socket.error as error:
                raise errors.NSQSocketError(*error)

            if not packet:
                raise errors.NSQSocketError(-1, 'failed to read %d' % size)

            self._buffer += packet

        data = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return data

    def _read_response(self):
        size = nsq.unpack_size(self._readn(4))
        return self._readn(size)

    def read_response(self):
        response    = self._read_response()
        frame, data = nsq.unpack_response(response)

        if frame not in self._frame_handlers:
            raise errors.NSQFrameError('unknown frame %s' % frame)

        frame_handler          = self._frame_handlers[frame]
        processed_data         = frame_handler(data)
        self._on_next_response = None

        return frame, processed_data

    def handle_response(self, data):
        if data == '_heartbeat_':
            self.nop()

        elif self._on_next_response is not None:
            self._on_next_response(self, response=data)

        self.on_response.send(self, response=data)
        return data

    def handle_error(self, data):
        error = errors.make_error(data)

        if self._on_next_response is not None:
            self._on_next_response(self, response=error)

        self.on_error.send(self, error=error)
        return error

    def handle_message(self, data):
        self.ready_count -= 1
        self.in_flight   += 1
        message = Message(self, *nsq.unpack_message(data))
        self.on_message.send(self, message=message)
        return message

    def listen(self):
        while self.is_connected:
            self.read_response()

    def identify(self,
        short_id           = SHORTNAME,
        long_id            = HOSTNAME,
        heartbeat_interval = None
    ):
        self.send(nsq.identify({
            'short_id':           short_id,
            'long_id':            long_id,
            'heartbeat_interval': heartbeat_interval
        }))

    def subscribe(self, topic, channel):
        self.send(nsq.subscribe(topic, channel))

    def publish_tcp(self, topic, data):
        self.send(nsq.publish(topic, data))

    def multipublish_tcp(self, topic, messages):
        self.send(nsq.multipublish(topic, messages))

    def ready(self, count):
        self.ready_count = count
        self.send(nsq.ready(count))

    def finish(self, message_id):
        self.send(nsq.finish(message_id))
        self.in_flight -= 1
        self.on_finish.send(self, message_id=message_id)

    def requeue(self, message_id, timeout=0):
        self.send(nsq.requeue(message_id, timeout))
        self.in_flight -= 1
        self.on_requeue.send(self, message_id=message_id, timeout=timeout)

    def touch(self, message_id):
        self.send(nsq.touch(message_id))

    def close(self):
        self.send(nsq.close())

    def nop(self):
        self.send(nsq.nop())

    @property
    def session(self):
        if self._session is None:
            self._session = requests.Session()
        return self._session

    @property
    def base_url(self):
        return 'http://%s:%s/' % (self.address, self.http_port)

    def url(self, *parts):
        return self.base_url + '/'.join(parts)

    def _check_api(self, *args, **kwargs):
        if not self.http_port:
            raise errors.NSQException(-1, 'no http port')

        resp = self.session.post(*args, **kwargs)
        if resp.status_code != 200:
            raise errors.NSQException(resp.status_code, 'api error')

        return resp.text

    def _json_api(self, *args, **kwargs):
        if not self.http_port:
            raise errors.NSQException(-1, 'no http port')

        resp = self.session.post(*args, **kwargs)
        if resp.status_code != 200:
            try:
                msg = resp.json()['status_txt']
            except:
                msg = 'api error'

            raise errors.NSQException(resp.status_code, msg)

        return resp.json()['data']


    def publish_http(self, topic, data):
        assert nsq.valid_topic_name(topic)
        return self._check_api(
            self.url('put'),
            params = {'topic': topic},
            data   = data
        )

    def multipublish_http(self, topic, messages):
        assert nsq.valid_topic_name(topic)

        for message in messages:
            if '\n' not in message:
                continue

            err = 'newlines are not allowed in http multipublish'
            raise errors.NSQException(-1, err)

        return self._check_api(
            self.url('mput'),
            params = {'topic': topic},
            data   = '\n'.join(messages)
        )

    def create_topic(self, topic):
        assert nsq.valid_topic_name(topic)
        return self._json_api(
            self.url('create_topic'),
            params = {'topic': topic}
        )

    def delete_topic(self, topic):
        assert nsq.valid_topic_name(topic)
        return self._json_api(
            self.url('delete_topic'),
            params = {'topic': topic}
        )

    def create_channel(self, topic, channel):
        assert nsq.valid_topic_name(topic)
        assert nsq.valid_channel_name(topic)
        return self._json_api(
            self.url('create_channel'),
            params = {'topic': topic, 'channel': channel}
        )

    def delete_channel(self, topic, channel):
        assert nsq.valid_topic_name(topic)
        assert nsq.valid_channel_name(topic)
        return self._json_api(
            self.url('delete_channel'),
            params = {'topic': topic, 'channel': channel}
        )

    def empty_topic(self, topic):
        assert nsq.valid_topic_name(topic)
        return self._json_api(
            self.url('empty_topic'),
            params = {'topic': topic}
        )

    def empty_channel(self, topic, channel):
        assert nsq.valid_topic_name(topic)
        assert nsq.valid_channel_name(topic)
        return self._json_api(
            self.url('empty_channel'),
            params = {'topic': topic, 'channel': channel}
        )

    def pause_channel(self, topic, channel):
        assert nsq.valid_topic_name(topic)
        assert nsq.valid_channel_name(topic)
        return self._json_api(
            self.url('pause_channel'),
            params = {'topic': topic, 'channel': channel}
        )

    def unpause_channel(self, topic, channel):
        assert nsq.valid_topic_name(topic)
        assert nsq.valid_channel_name(topic)
        return self._json_api(
            self.url('unpause_channel'),
            params = {'topic': topic, 'channel': channel}
        )

    def stats(self):
        return self._json_api(self.url('stats'), params={'format': 'json'})

    def ping(self):
        return self._check_api(self.url('ping'))

    def info(self):
        return self._json_api(self.url('info'))

    def publish(self, topic, data):
        if self.is_connected:
            return self.publish_tcp(topic, data)
        else:
            return self.publish_http(topic, data)

    def multipublish(self, topic, messages):
        if self.is_connected:
            return self.multipublish_tcp(topic, messages)
        else:
            return self.multipublish_http(topic, messages)

    def __str__(self):
        return self.address + ':' + str(self.tcp_port)

    def __hash__(self):
        return hash((self.address, self.tcp_port))

    def __eq__(self, other):
        return (
            isinstance(other, Nsqd)         and
            self.address  == other.address  and
            self.tcp_port == other.tcp_port
        )

    def __cmp__(self, other):
        return hash(self) - hash(other)
