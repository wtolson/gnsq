import json
import struct

import pytest
import six

from gnsq import protocol as nsq


IDENTIFY_DICT_ASCII = {'a': 1, 'b': 2}
IDENTIFY_DICT_UNICODE = {'c': u'w\xc3\xa5\xe2\x80\xa0'}
IDENTIFY_BODY_ASCII = six.b(json.dumps(IDENTIFY_DICT_ASCII))
IDENTIFY_BODY_UNICODE = six.b(json.dumps(IDENTIFY_DICT_UNICODE))

MSGS = [b'asdf', b'ghjk', b'abcd']
MPUB_BODY = struct.pack('>l', len(MSGS)) + b''.join(struct.pack('>l', len(m)) + m for m in MSGS)


@pytest.mark.parametrize('cmd_method,kwargs,result', [
    ('identify',
        {'data': IDENTIFY_DICT_ASCII},
        b'IDENTIFY\n' + struct.pack('>l', len(IDENTIFY_BODY_ASCII)) +
        IDENTIFY_BODY_ASCII),
    ('identify',
        {'data': IDENTIFY_DICT_UNICODE},
        b'IDENTIFY\n' + struct.pack('>l', len(IDENTIFY_BODY_UNICODE)) +
        IDENTIFY_BODY_UNICODE),
    ('auth',
        {'secret': b'secret'},
        b'AUTH\n' + struct.pack('>l', 6) + b'secret'),
    ('subscribe',
        {'topic_name': 'test_topic', 'channel_name': 'test_channel'},
        b'SUB test_topic test_channel\n'),
    ('finish',
        {'message_id': 'test'},
        b'FIN test\n'),
    ('finish',
        {'message_id': u'\u2020est \xfcn\xee\xe7\xf8\u2202\xe9'},
        b'FIN \xe2\x80\xa0est \xc3\xbcn\xc3\xae\xc3\xa7\xc3\xb8\xe2\x88\x82\xc3\xa9\n'),
    ('requeue',
        {'message_id': 'test'},
        b'REQ test 0\n'),
    ('requeue',
        {'message_id': 'test', 'timeout': 60},
        b'REQ test 60\n'),
    ('touch',
        {'message_id': 'test'},
        b'TOUCH test\n'),
    ('ready',
        {'count': 100},
        b'RDY 100\n'),
    ('nop',
        {},
        b'NOP\n'),
    ('publish',
        {'topic_name': 'test', 'data': MSGS[0]},
        b'PUB test\n' + struct.pack('>l', len(MSGS[0])) + MSGS[0]),
    ('multipublish',
        {'topic_name': 'test', 'messages': MSGS},
        b'MPUB test\n' + struct.pack('>l', len(MPUB_BODY)) + MPUB_BODY),
    ('deferpublish',
        {'topic_name': 'test', 'data': MSGS[0], 'delay_ms': 42},
        b'DPUB test 42\n' + struct.pack('>l', len(MSGS[0])) + MSGS[0]),
])
def test_command(cmd_method, kwargs, result):
    assert getattr(nsq, cmd_method)(**kwargs) == result


def test_unicode_body():
    pytest.raises(TypeError, nsq.publish, 'topic', u'unicode body')
