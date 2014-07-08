from __future__ import with_statement

import pytest
import gnsq
from integration_server import NsqdIntegrationServer


@pytest.mark.slow
def test_basic():
    with NsqdIntegrationServer() as server:
        conn = gnsq.Nsqd(server.address, http_port=server.http_port)
        assert conn.ping() == 'OK'
        assert 'topics' in conn.stats()
        assert 'version' in conn.info()


@pytest.mark.slow
def test_topics_channels():
    with NsqdIntegrationServer() as server:
        conn = gnsq.Nsqd(server.address, http_port=server.http_port)
        assert len(conn.stats()['topics']) == 0

        with pytest.raises(gnsq.errors.NSQHttpError):
            conn.delete_topic('topic')

        conn.create_topic('topic')
        topics = conn.stats()['topics']
        assert len(topics) == 1
        assert topics[0]['topic_name'] == 'topic'

        conn.delete_topic('topic')
        assert len(conn.stats()['topics']) == 0

        with pytest.raises(gnsq.errors.NSQHttpError):
            conn.create_channel('topic', 'channel')

        with pytest.raises(gnsq.errors.NSQHttpError):
            conn.delete_channel('topic', 'channel')

        conn.create_topic('topic')
        assert len(conn.stats()['topics'][0]['channels']) == 0

        conn.create_channel('topic', 'channel')
        channels = conn.stats()['topics'][0]['channels']
        assert len(channels) == 1
        assert channels[0]['channel_name'] == 'channel'

        conn.delete_channel('topic', 'channel')
        assert len(conn.stats()['topics'][0]['channels']) == 0


@pytest.mark.slow
def test_publish():
    with NsqdIntegrationServer() as server:
        conn = gnsq.Nsqd(server.address, http_port=server.http_port)

        conn.publish('topic', 'sup')
        assert conn.stats()['topics'][0]['depth'] == 1

        conn.multipublish('topic', ['sup', 'sup'])
        assert conn.stats()['topics'][0]['depth'] == 3

        conn.multipublish('topic', iter(['sup', 'sup', 'sup']))
        assert conn.stats()['topics'][0]['depth'] == 6

        conn.empty_topic('topic')
        assert conn.stats()['topics'][0]['depth'] == 0

        conn.create_topic('topic')
        conn.create_channel('topic', 'channel')
        conn.publish('topic', 'sup')
        assert conn.stats()['topics'][0]['channels'][0]['depth'] == 1

        conn.empty_channel('topic', 'channel')
        assert conn.stats()['topics'][0]['channels'][0]['depth'] == 0
