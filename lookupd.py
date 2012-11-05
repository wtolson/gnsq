import requests
from .util import assert_list

class Lookupd(object):
    def __init__(self, addresses):
        self.addresses = assert_list(addresses)

    def lookup(self, topic):
        producers = []
        for address in self.addresses:
            producers.extend(self._lookup(address, topic))

        return producers

    def iter_lookup(self, topic):
        for address in self.addresses:
            for producer in self._lookup(address, topic):
                yield producer

    def _lookup(self, address, topic):
        url  = '%s/lookup' % address
        resp = requests.get(url, params={'topic': topic})

        if resp.status_code != 200:
            return []

        print resp.json['data']
        return resp.json['data']['producers']
