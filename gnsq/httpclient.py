# -*- coding: utf-8 -*-
from __future__ import absolute_import

import urllib3

try:
    import simplejson as json
except ImportError:
    import json  # pyflakes.ignore

from .decorators import cached_property
from .errors import NSQHttpError


class HTTPClient(object):
    @cached_property
    def http(self):
        return urllib3.connection_from_url(url=self.base_url)

    def http_request(self, method, url, **kwargs):
        response = self.http.request_encode_url(method, url, **kwargs)

        if 'application/json' in response.getheader('content-type', ''):
            return self._http_check_json(response)

        return self._http_check(response)

    def _http_check(self, response):
        if response.status != 200:
            raise NSQHttpError('http error <%s>' % response.status)
        return response.data

    def _http_check_json(self, response):
        try:
            data = json.loads(response.data.decode('utf-8'))
        except ValueError:
            return self._http_check(response)

        if response.status != 200:
            status_txt = data.get('status_txt', 'http error')
            raise NSQHttpError('%s <%s>' % (status_txt, response.status))

        # Handle 1.0.0-compat vs 0.x versions
        if 'data' in data:
            return data['data']
        else:
            return data

    def http_get(self, url, **kwargs):
        return self.http_request('GET', url, **kwargs)

    def http_post(self, url, **kwargs):
        return self.http_request('POST', url, **kwargs)
