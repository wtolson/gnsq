# -*- coding: utf-8 -*-
from __future__ import absolute_import
from .httpclient import HTTPClient
from . import protocal as nsq


class Lookupd(HTTPClient):
    def __init__(self, address='http://localhost:4161/'):
        if not address.endswith('/'):
            address += '/'

        self.address = self.base_url = address

    def lookup(self, topic):
        nsq.assert_valid_topic_name(topic)
        return self._json_api(
            self.url('lookup'),
            params={'topic': topic}
        )

    def topics(self):
        return self._json_api(self.url('topics'))

    def channels(self, topic):
        nsq.assert_valid_topic_name(topic)
        return self._json_api(
            self.url('channels'),
            params={'topic': topic}
        )

    def nodes(self):
        return self._json_api(self.url('nodes'))

    def delete_topic(self, topic):
        nsq.assert_valid_topic_name(topic)
        return self._json_api(
            self.url('delete_topic'),
            params={'topic': topic}
        )

    def delete_channel(self, topic, channel):
        nsq.assert_valid_topic_name(topic)
        nsq.assert_valid_channel_name(channel)
        return self._json_api(
            self.url('delete_channel'),
            params={'topic': topic, 'channel': channel}
        )

    def tombstone_topic_producer(self, topic, node):
        nsq.assert_valid_topic_name(topic)
        return self._json_api(
            self.url('tombstone_topic_producer'),
            params={'topic': topic, 'node': node}
        )

    def ping(self):
        return self._check_api(self.url('ping'))

    def info(self):
        return self._json_api(self.url('info'))
