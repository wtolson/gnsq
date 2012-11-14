import re
import requests
import blinker

import gevent
from gevent import socket
from gevent.queue import Queue
from gevent.event import AsyncResult

from . import protocal as nsq
from . import errors

from .message import Message

HOSTNAME  = socket.gethostname()
SHORTNAME = HOSTNAME.split('.')[0]

class Nsqd(object):
    def __init__(self, address, tcp_port=None, http_port=None, timeout=120):
        self.address   = address
        self.tcp_port  = tcp_port
        self.http_port = http_port
        self.timeout   = timeout

        self.on_response = blinker.Signal()
        self.on_error    = blinker.Signal()
        self.on_message  = blinker.Signal()
        self.on_finish   = blinker.Signal()
        self.on_requeue  = blinker.Signal()

        self._send_worker = gevent.spawn(self._send)
        self._send_queue  = Queue()

        self._frame_handlers = {
            nsq.FRAME_TYPE_RESPONSE: self.handle_response,
            nsq.FRAME_TYPE_ERROR:    self.handle_error,
            nsq.FRAME_TYPE_MESSAGE:  self.handle_message
        }

        self.reset()

    def stats(self):
        url  = 'http://%s:%s/stats?format=json' % (self.address, self.http_port)
        resp = requests.get(url)

        if resp.status_code != 200:
            return None

        return resp.json['data']

    def connect(self):
        if self._socket is not None:
            raise NSQException('Already connected.')

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self.timeout)
        self._socket.connect((self.address, self.tcp_port))
        self._socket.send(nsq.MAGIC_V2)

    def send(self, data, async=False):
        result = AsyncResult()
        self._send_queue.put((data, result))

        if async:
            return result

        result.get()

    def _send(self):
        while 1:
            data, result = self._send_queue.get()
            try:
                self._socket.send(data)
                result.set(True)

            except socket.error as error:
                result.set_exception(errors.NSQSocketError(str(error)))

            except Exception as error:
                result.set_exception(error)

    def reset(self):
        self.ready_count       = 0
        self.in_flight         = 0
        self._buffer           = ''
        self._socket           = None
        self._on_next_response = None

    def _readn(self, size):
        while len(self._buffer) < size:
            packet = self._socket.recv(4096)
            if not packet:
                raise errors.NSQSocketError("failed to read %d" % size)

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
        while self._socket:
            frame, data = self.read_response()

            if frame != nsq.FRAME_TYPE_RESPONSE:
                continue

            if data == 'CLOSE_WAIT':
                break

    def subscribe(self, topic, channel, short_id=SHORTNAME, long_id=HOSTNAME):
        self.send(nsq.subscribe(topic, channel, short_id, long_id))

    def publish(self, topic, data):
        self.send(nsq.publish(topic, data))

    def multipublish(topic, messages):
        self.send(nsq.multipublish(topic, messages))

    def ready(self, count):
        self.ready_count = count
        self.send(nsq.ready(count))

    def finish(self, message_id):
        self.send(nsq.finish(message_id))
        self.in_flight -= 1
        self.on_finish.send(self, message_id=message_id)

    def requeue(self, message_id, timeout):
        self.send(nsq.requeue(message_id, timeout))
        self.in_flight -= 1
        self.on_requeue.send(self, message_id=message_id, timeout=timeout)

    def close(self):
        self.send(nsq.close())

    def nop(self):
        self.send(nsq.nop())

    def __str__(self):
        return self.address + ':' + str(self.tcp_port)

    def __hash__(self):
        return hash((self.address, self.tcp_port))

    def __eq__(self, other):
        return (
            isinstance(other, Connection)   and
            self.address  == other.address  and
            self.tcp_port == other.tcp_port
        )

    def __cmp__(self, other):
        return hash(self) - hash(other)
