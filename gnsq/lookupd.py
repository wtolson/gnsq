# -*- coding: utf-8 -*-
from __future__ import absolute_import
from .httpclient import HTTPClient
from . import protocol as nsq


class Lookupd(HTTPClient):
    """Low level client for nsqlookupd.

    :param address: nsqlookupd http address (default: http://localhost:4161/)
    """
    def __init__(self, address='http://localhost:4161/'):
        self.address = self.base_url = address

    def lookup(self, topic):
        """Returns producers for a topic."""
        nsq.assert_valid_topic_name(topic)
        return self.http_get('/lookup', fields={'topic': topic})

    def topics(self):
        """Returns all known topics."""
        return self.http_get('/topics')

    def channels(self, topic):
        """Returns all known channels of a topic."""
        nsq.assert_valid_topic_name(topic)
        return self.http_get('/channels', fields={'topic': topic})

    def nodes(self):
        """Returns all known nsqd."""
        return self.http_get('/nodes')

    def delete_topic(self, topic):
        """Deletes an existing topic."""
        nsq.assert_valid_topic_name(topic)
        return self.http_post('/topic/delete', fields={'topic': topic})

    def delete_channel(self, topic, channel):
        """Deletes an existing channel of an existing topic."""
        nsq.assert_valid_topic_name(topic)
        nsq.assert_valid_channel_name(channel)
        return self.http_post(
            url='/channel/delete',
            fields={'topic': topic, 'channel': channel},
        )

    def tombstone_topic_producer(self, topic, node):
        """Tombstones a specific producer of an existing topic."""
        nsq.assert_valid_topic_name(topic)
        return self.http_post(
            url='/tombstone_topic_producer',
            fields={'topic': topic, 'node': node},
        )

    def ping(self):
        """Monitoring endpoint.

        :returns: should return `"OK"`, otherwise raises an exception.
        """
        return self.http_get('/ping')

    def info(self):
        """Returns version information."""
        return self.http_get('/info')
