import random

import six

from .lookupd import LookupdClient


def normalize_nsqd_address(address):
    if not isinstance(address, six.string_types):
        raise TypeError('nsqd address must be a string')

    host, _, port = address.partition(':')
    if port:
        try:
            port = int(port, 10)
        except ValueError:
            raise ValueError('invalid nsqd port')

    else:
        port = 4150

    return '{host}:{port}'.format(host=host, port=port)


def parse_nsqds(nsqd_tcp_addresses):
    if isinstance(nsqd_tcp_addresses, six.string_types):
        return set([normalize_nsqd_address(nsqd_tcp_addresses)])

    elif isinstance(nsqd_tcp_addresses, (list, tuple, set)):
        return set(normalize_nsqd_address(addr) for addr in nsqd_tcp_addresses)

    raise TypeError('nsqd_tcp_addresses must be a list, set or tuple')


def parse_lookupds(lookupd_http_addresses):
    if isinstance(lookupd_http_addresses, six.string_types):
        return [LookupdClient.from_url(lookupd_http_addresses)]

    if not isinstance(lookupd_http_addresses, (list, tuple)):
        msg = 'lookupd_http_addresses must be a list, set or tuple'
        raise TypeError(msg)

    lookupd = [LookupdClient.from_url(a) for a in lookupd_http_addresses]
    random.shuffle(lookupd)

    return lookupd
