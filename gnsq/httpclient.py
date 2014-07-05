from __future__ import absolute_import
import requests
from .errors import NSQException


class HTTPClient(object):
    base_url = None
    _session = None

    @property
    def session(self):
        if self._session is None:
            self._session = requests.Session()
        return self._session

    def url(self, *parts):
        return self.base_url + '/'.join(parts)

    def _check_connection(self):
        pass

    def _check_api(self, *args, **kwargs):
        self._check_connection()

        resp = self.session.post(*args, **kwargs)
        if resp.status_code != 200:
            raise NSQException(resp.status_code, 'api error')

        return resp.text

    def _json_api(self, *args, **kwargs):
        self._check_connection()

        resp = self.session.post(*args, **kwargs)
        if resp.status_code != 200:
            try:
                msg = resp.json()['status_txt']
            except:
                msg = 'api error'

            raise NSQException(resp.status_code, msg)

        return resp.json()['data']
