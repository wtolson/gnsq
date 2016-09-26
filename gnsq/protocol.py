# -*- coding: utf-8 -*-
from __future__ import absolute_import

import re
import struct
import six

try:
    import simplejson as json
except ImportError:
    import json  # pyflakes.ignore


__all__ = [
    'MAGIC_V2',
    'FRAME_TYPE_RESPONSE',
    'FRAME_TYPE_ERROR',
    'FRAME_TYPE_MESSAGE',
    'unpack_size',
    'unpack_response',
    'unpack_message',
    'subscribe',
    'publish',
    'ready',
    'finish',
    'requeue',
    'close',
    'nop'
]

MAGIC_V2 = b'  V2'
NEWLINE = b'\n'
SPACE = b' '
EMPTY = b''
HEARTBEAT = b'_heartbeat_'
OK = b'OK'

IDENTIFY = b'IDENTIFY'
AUTH = b'AUTH'
SUB = b'SUB'
PUB = b'PUB'
MPUB = b'MPUB'
DPUB = b'DPUB'
RDY = b'RDY'
FIN = b'FIN'
REQ = b'REQ'
TOUCH = b'TOUCH'
CLS = b'CLS'
NOP = b'NOP'

FRAME_TYPE_RESPONSE = 0
FRAME_TYPE_ERROR = 1
FRAME_TYPE_MESSAGE = 2


#
# Helpers
#
VALID_NAME_RE = re.compile(r'^[\.a-zA-Z0-9_-]+(#ephemeral)?$')


def _valid_name(name):
    if not 0 < len(name) < 65:
        return False
    return bool(VALID_NAME_RE.match(name))


def valid_topic_name(topic):
    return _valid_name(topic)


def valid_channel_name(channel):
    return _valid_name(channel)


def assert_valid_topic_name(topic):
    if valid_topic_name(topic):
        return
    raise ValueError('invalid topic name')


def assert_valid_channel_name(channel):
    if valid_channel_name(channel):
        return
    raise ValueError('invalid channel name')


#
# Responses
#
def unpack_size(data):
    return struct.unpack('>l', data)[0]


def unpack_response(data):
    return unpack_size(data[:4]), data[4:]


def unpack_message(data):
    timestamp = struct.unpack('>q', data[:8])[0]
    attempts = struct.unpack('>h', data[8:10])[0]
    message_id = data[10:26]
    body = data[26:]
    return timestamp, attempts, message_id, body


#
# Commands
#
def _packsize(data):
    return struct.pack('>l', len(data))


def _packbody(body):
    if body is None:
        return EMPTY
    if not isinstance(body, bytes):
        raise TypeError('message body must be a byte string')
    return _packsize(body) + body


def _encode_param(data):
    if isinstance(data, bytes):
        return data
    return data.encode('utf-8')


def _command(cmd, body, *params):
    params = tuple(_encode_param(p) for p in params)
    return EMPTY.join((SPACE.join((cmd,) + params), NEWLINE, _packbody(body)))


def identify(data):
    return _command(IDENTIFY, six.b(json.dumps(data)))


def auth(secret):
    return _command(AUTH, secret)


def subscribe(topic_name, channel_name):
    assert_valid_topic_name(topic_name)
    assert_valid_channel_name(channel_name)
    return _command(SUB, None, topic_name, channel_name)


def publish(topic_name, data):
    assert_valid_topic_name(topic_name)
    return _command(PUB, data, topic_name)


def multipublish(topic_name, messages):
    assert_valid_topic_name(topic_name)
    data = EMPTY.join(_packbody(m) for m in messages)
    return _command(MPUB, _packsize(messages) + data, topic_name)


def deferpublish(topic_name, data, delay_ms):
    assert_valid_topic_name(topic_name)
    return _command(DPUB, data, topic_name, six.b('%d' % delay_ms))


def ready(count):
    if not isinstance(count, int):
        raise TypeError('ready count must be an integer')

    if count < 0:
        raise ValueError('ready count cannot be negative')

    return _command(RDY, None, '%d' % count)


def finish(message_id):
    return _command(FIN, None, message_id)


def requeue(message_id, timeout=0):
    if not isinstance(timeout, int):
        raise TypeError('requeue timeout must be an integer')
    return _command(REQ, None, message_id, '%d' % timeout)


def touch(message_id):
    return _command(TOUCH, None, message_id)


def close():
    return _command(CLS, None)


def nop():
    return _command(NOP, None)
