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
                (nsq.identify,
                    {'data': identify_dict_ascii},
                    'IDENTIFY\n' + struct.pack('>l', len(identify_body_ascii)) +
                    identify_body_ascii),
                (nsq.identify,
                    {'data': identify_dict_unicode},
                    'IDENTIFY\n' + struct.pack('>l', len(identify_body_unicode)) +
                    identify_body_unicode),
                (nsq.subscribe,
                    {'topic_name': 'test_topic', 'channel_name': 'test_channel'},
                    'SUB test_topic test_channel\n'),
                (nsq.finish,
                    {'message_id': 'test'},
                    'FIN test\n'),
                (nsq.finish,
                    {'message_id': u'\u2020est \xfcn\xee\xe7\xf8\u2202\xe9'},
                    'FIN \xe2\x80\xa0est \xc3\xbcn\xc3\xae\xc3\xa7\xc3\xb8\xe2\x88\x82\xc3\xa9\n'),
                (nsq.requeue,
                    {'message_id': 'test'},
                    'REQ test 0\n'),
                (nsq.requeue,
                    {'message_id': 'test', 'timeout': 60},
                    'REQ test 60\n'),
                (nsq.touch,
                    {'message_id': 'test'},
                    'TOUCH test\n'),
                (nsq.ready,
                    {'count': 100},
                    'RDY 100\n'),
                (nsq.nop,
                    {},
                    'NOP\n'),
                (nsq.publish,
                    {'topic_name': 'test', 'data': msgs[0]},
                    'PUB test\n' + struct.pack('>l', len(msgs[0])) + msgs[0]),
                (nsq.multipublish,
                    {'topic_name': 'test', 'messages': msgs},
                    'MPUB test\n' + struct.pack('>l', len(mpub_body)) + mpub_body)
                ]:
            metafunc.addcall(funcargs=dict(cmd_method=cmd_method, kwargs=kwargs, result=result))


def test_command(cmd_method, kwargs, result):
    assert cmd_method(**kwargs) == result


def test_unicode_body():
    pytest.raises(TypeError, nsq.publish, 'topic', u'unicode body')
