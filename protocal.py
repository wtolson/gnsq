import re
import struct

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
TOPIC_NAME_RE   = re.compile('^[\.a-zA-Z0-9_-]+$')
CHANNEL_NAME_RE = re.compile('^[\.a-zA-Z0-9_-]+(#ephemeral)?$')

def is_valid_topic_name(name):
    length = len(name)
    if length > 32 or length < 1:
        return False

    return bool(TOPIC_NAME_RE.match(name))

def is_valid_channel_name(name):
    length = len(name)
    if length > 32 or length < 1:
        return False

    return bool(CHANNEL_NAME_RE.match(name))

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
def _command(*params):
    assert len(params) > 0
    return ' '.join(params) + NEWLINE

def subscribe(topic_name, channel_name, short_id, long_id):
    assert is_valid_topic_name(topic_name)
    assert is_valid_channel_name(channel_name)
    return _command('SUB', topic_name, channel_name, short_id, long_id)

def publish(topic_name, data):
    assert is_valid_topic_name(topic_name)
    return ''.join([
        _command('PUB', topic_name),
        struct.pack('>l', len(data)),
        data
    ])

def ready(count):
    return _command('RDY', str(count))

def finish(message_id):
    return _command('FIN', message_id)

def requeue(message_id, timeout):
    return _command('REQ', message_id, str(timeout))

def close():
    return _command('CLS')

def nop():
    return _command('NOP')
