# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

import urllib3

from .errors import NSQHttpError
from .version import __version__

USERAGENT = 'gnsq/{}'.format(__version__)


def _encode(value):
    if isinstance(value, bytes):
        return value
    return value.encode('utf-8')


def _encode_dict(value):
    if value is None:
        return None
    return {_encode(k): _encode(v) for k, v in value.items()}


class HTTPClient(object):
    @classmethod
    def from_url(cls, url, **kwargs):
        """Create a client from a url."""
        url = urllib3.util.parse_url(url)
        if url.host:
            kwargs.setdefault('host', url.host)

        if url.port:
            kwargs.setdefault('port', url.port)

        if url.scheme == 'https':
            kwargs.setdefault('connection_class', urllib3.HTTPSConnectionPool)

        return cls(**kwargs)

    def __init__(self, host, port, useragent=USERAGENT,
                 connection_class=urllib3.HTTPConnectionPool, **kwargs):
        self.useragent = useragent
        self._connection = connection_class(host, port, **kwargs)

    @property
    def scheme(self):
        return self._connection.scheme

    @property
    def host(self):
        return self._connection.host

    @property
    def port(self):
        return self._connection.port

    @property
    def address(self):
        return '{}://{}:{}/'.format(self.scheme, self.host, self.port)

    def _request(self, method, url, headers={}, fields=None, **kwargs):
        headers = dict(headers)
        headers.setdefault('Accept', 'application/vnd.nsq version=1.0')
        headers.setdefault('User-Agent', self.useragent)

        response = self._connection.request_encode_url(
            method, url, headers=_encode_dict(headers),
            fields=_encode_dict(fields), **kwargs)

        if 'application/json' in response.getheader('content-type', ''):
            return self._http_check_json(response)

        return self._http_check(response)

    def _http_check(self, response):
        if response.status != 200:
            raise NSQHttpError('http error <{}>'.format(response.status))
        return response.data

    def _http_check_json(self, response):
        try:
            data = json.loads(response.data.decode('utf-8'))
        except ValueError:
            return self._http_check(response)

        if response.status != 200:
            status_txt = data.get('status_txt', 'http error')
            raise NSQHttpError('{} <{}>'.format(status_txt, response.status))

        # Handle 1.0.0-compat vs 0.x versions
        try:
            data['data']
        except KeyError:
            return data

    def __repr__(self):
        return '<{!s} {!r}>'.format(type(self).__name__, self.address)
