# -*- coding: utf-8 -*-
from __future__ import absolute_import

import urllib3

from . import protocol as nsq
from .decorators import deprecated
from .httpclient import HTTPClient


class LookupdClient(HTTPClient):
    """Low level http client for nsqlookupd.

    :param host: nsqlookupd host address (default: localhost)

    :param port: nsqlookupd http port (default: 4161)

    :param useragent: useragent sent to nsqlookupd (default:
        ``<client_library_name>/<version>``)

    :param connection_class: override the http connection class
    """
    def __init__(self, host='localhost', port=4161, **kwargs):
        super(LookupdClient, self).__init__(host, port, **kwargs)

    def lookup(self, topic):
        """Returns producers for a topic."""
        nsq.assert_valid_topic_name(topic)
        return self._request('GET', '/lookup', fields={'topic': topic})

    def topics(self):
        """Returns all known topics."""
        return self._request('GET', '/topics')

    def channels(self, topic):
        """Returns all known channels of a topic."""
        nsq.assert_valid_topic_name(topic)
        return self._request('GET', '/channels', fields={'topic': topic})

    def nodes(self):
        """Returns all known nsqd."""
        return self._request('GET', '/nodes')

    def create_topic(self, topic):
        """Add a topic to nsqlookupd's registry."""
        nsq.assert_valid_topic_name(topic)
        return self._request('POST', '/topic/create', fields={'topic': topic})

    def delete_topic(self, topic):
        """Deletes an existing topic."""
        nsq.assert_valid_topic_name(topic)
        return self._request('POST', '/topic/delete', fields={'topic': topic})

    def create_channel(self, topic, channel):
        """Add a channel to nsqlookupd's registry."""
        nsq.assert_valid_topic_name(topic)
        nsq.assert_valid_channel_name(channel)
        return self._request('POST', '/channel/create',
                             fields={'topic': topic, 'channel': channel})

    def delete_channel(self, topic, channel):
        """Deletes an existing channel of an existing topic."""
        nsq.assert_valid_topic_name(topic)
        nsq.assert_valid_channel_name(channel)
        return self._request('POST', '/channel/delete',
                             fields={'topic': topic, 'channel': channel})

    def tombstone_topic(self, topic, node):
        """Tombstones a specific producer of an existing topic."""
        nsq.assert_valid_topic_name(topic)
        return self._request('POST', '/topic/tombstone',
                             fields={'topic': topic, 'node': node})

    def ping(self):
        """Monitoring endpoint.

        :returns: should return `"OK"`, otherwise raises an exception.
        """
        return self._request('GET', '/ping')

    def info(self):
        """Returns version information."""
        return self._request('GET', '/info')


class Lookupd(LookupdClient):
    """Use :class:`LookupdClient` instead.

    .. deprecated:: 1.0.0
    """

    @deprecated
    def __init__(self, address='http://localhost:4161/', **kwargs):
        """Use :meth:`LookupdClient.from_url` instead.

        .. deprecated:: 1.0.0
        """
        self.address = self.base_url = address

        url = urllib3.util.parse_url(address)
        if url.host:
            kwargs.setdefault('host', url.host)

        if url.port:
            kwargs.setdefault('port', url.port)

        if url.scheme == 'https':
            kwargs.setdefault('connection_class', urllib3.HTTPSConnectionPool)

        return super(Lookupd, self).__init__(**kwargs)

    @property
    @deprecated
    def base_url(self):
        """Use :attr:`LookupdClient.address` instead.

        .. deprecated:: 1.0.0
        """
        return self.address

    @deprecated
    def tombstone_topic_producer(self, topic, node):
        """Use :meth:`LookupdClient.tombstone_topic` instead.

        .. deprecated:: 1.0.0
        """
        return self.tombstone_topic(topic, node)
