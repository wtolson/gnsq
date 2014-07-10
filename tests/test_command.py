import struct
import pytest

try:
    import simplejson as json
except ImportError:
    import json  # pyflakes.ignore

from gnsq import protocol as nsq


def pytest_generate_tests(metafunc):
    identify_dict_ascii = {'a': 1, 'b': 2}
    identify_dict_unicode = {'c': u'w\xc3\xa5\xe2\x80\xa0'}
    identify_body_ascii = json.dumps(identify_dict_ascii)
    identify_body_unicode = json.dumps(identify_dict_unicode)

    msgs = ['asdf', 'ghjk', 'abcd']
    mpub_body = struct.pack('>l', len(msgs)) + ''.join(struct.pack('>l', len(m)) + m for m in msgs)
    if metafunc.function == test_command:
        for cmd_method, kwargs, result in [
                ('identify',
                    {'data': identify_dict_ascii},
                    'IDENTIFY\n' + struct.pack('>l', len(identify_body_ascii)) +
                    identify_body_ascii),
                ('identify',
                    {'data': identify_dict_unicode},
                    'IDENTIFY\n' + struct.pack('>l', len(identify_body_unicode)) +
                    identify_body_unicode),
                ('auth',
                    {'secret': 'secret'},
                    'AUTH\n' + struct.pack('>l', 6) + 'secret'),
                ('subscribe',
                    {'topic_name': 'test_topic', 'channel_name': 'test_channel'},
                    'SUB test_topic test_channel\n'),
                ('finish',
                    {'message_id': 'test'},
                    'FIN test\n'),
                ('finish',
                    {'message_id': u'\u2020est \xfcn\xee\xe7\xf8\u2202\xe9'},
                    'FIN \xe2\x80\xa0est \xc3\xbcn\xc3\xae\xc3\xa7\xc3\xb8\xe2\x88\x82\xc3\xa9\n'),
                ('requeue',
                    {'message_id': 'test'},
                    'REQ test 0\n'),
                ('requeue',
                    {'message_id': 'test', 'timeout': 60},
                    'REQ test 60\n'),
                ('touch',
                    {'message_id': 'test'},
                    'TOUCH test\n'),
                ('ready',
                    {'count': 100},
                    'RDY 100\n'),
                ('nop',
                    {},
                    'NOP\n'),
                ('publish',
                    {'topic_name': 'test', 'data': msgs[0]},
                    'PUB test\n' + struct.pack('>l', len(msgs[0])) + msgs[0]),
                ('multipublish',
                    {'topic_name': 'test', 'messages': msgs},
                    'MPUB test\n' + struct.pack('>l', len(mpub_body)) + mpub_body)
                ]:
            metafunc.addcall(funcargs=dict(cmd_method=cmd_method, kwargs=kwargs, result=result))


def test_command(cmd_method, kwargs, result):
    assert getattr(nsq, cmd_method)(**kwargs) == result


def test_unicode_body():
    pytest.raises(TypeError, nsq.publish, 'topic', u'unicode body')
