import re
import struct
import json

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

MAGIC_V2 = '  V2'
NEWLINE  = '\n'

FRAME_TYPE_RESPONSE = 0
FRAME_TYPE_ERROR    = 1
FRAME_TYPE_MESSAGE  = 2


#
# Helpers
#
TOPIC_NAME_RE   = re.compile(r'^[\.a-zA-Z0-9_-]+$')
CHANNEL_NAME_RE = re.compile(r'^[\.a-zA-Z0-9_-]+(#ephemeral)?$')

def valid_topic_name(topic):
    if not 0 < len(topic) < 33:
        return False
    return bool(TOPIC_NAME_RE.match(topic))

def valid_channel_name(channel):
    if not 0 < len(channel) < 33:
        return False
    return bool(CHANNEL_NAME_RE.match(channel))

def assert_valid_topic_name(topic):
    assert valid_topic_name(topic)

def assert_valid_channel_name(channel):
    assert valid_channel_name(channel)

#
# Responses
#
def unpack_size(data):
    assert len(data) == 4
    return struct.unpack('>l', data)[0]

def unpack_response(data):
    return unpack_size(data[:4]), data[4:]

def unpack_message(data):
    timestamp  = struct.unpack('>q', data[:8])[0]
    attempts   = struct.unpack('>h', data[8:10])[0]
    message_id = data[10:26]
    body       = data[26:]
    return timestamp, attempts, message_id, body

#
# Commands
#
def _packbody(body):
    if body is None:
        return ''
    return struct.pack('>l', len(body)) + body

def _command(cmd, body, *params):
    return ''.join((' '.join((cmd,) + params), NEWLINE, _packbody(body)))

def identify(data):
    return _command('IDENTIFY', json.dumps(data))

def subscribe(topic_name, channel_name):
    assert_valid_topic_name(topic_name)
    assert_valid_channel_name(channel_name)
    return _command('SUB', None, topic_name, channel_name)

def publish(topic_name, data):
    assert_valid_topic_name(topic_name)
    return _command('PUB', data, topic_name)

def multipublish(topic_name, messages):
    assert_valid_topic_name(topic_name)
    data = ''.join(_packbody(m) for m in messages)
    return _command('MPUB', data, topic_name)

def ready(count):
    assert isinstance(count, int), "ready count must be an integer"
    assert count >= 0, "ready count cannot be negative"
    return _command('RDY', None, str(count))

def finish(message_id):
    return _command('FIN', None, message_id)

def requeue(message_id, timeout=0):
    assert isinstance(timeout, int), "requeue timeout must be an integer"
    return _command('REQ', None, message_id, str(timeout))

def touch(message_id):
    return _command('TOUCH', None, message_id)

def close():
    return _command('CLS', None)

def nop():
    return _command('NOP', None)
