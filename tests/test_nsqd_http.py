from __future__ import with_statement

import pytest
import gnsq
from integration_server import NsqdIntegrationServer


@pytest.mark.slow
@pytest.mark.timeout(60)
def test_basic():
    with NsqdIntegrationServer() as server:
        conn = gnsq.NsqdHTTPClient(server.address, server.http_port)
        assert conn.ping() == b'OK'
        assert 'topics' in conn.stats()
        assert 'version' in conn.info()


@pytest.mark.slow
@pytest.mark.timeout(60)
def test_topics_channels():
    with NsqdIntegrationServer() as server:
        conn = gnsq.NsqdHTTPClient(server.address, server.http_port)
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
@pytest.mark.timeout(60)
def test_publish():
    with NsqdIntegrationServer() as server:
        conn = gnsq.NsqdHTTPClient(server.address, server.http_port)

        conn.publish('topic', b'sup')
        assert conn.stats()['topics'][0]['depth'] == 1

        conn.multipublish('topic', [b'sup', b'sup'])
        assert conn.stats()['topics'][0]['depth'] == 3

        conn.multipublish('topic', iter([b'sup', b'sup', b'sup']))
        assert conn.stats()['topics'][0]['depth'] == 6

        conn.empty_topic('topic')
        assert conn.stats()['topics'][0]['depth'] == 0

        conn.create_topic('topic')
        conn.create_channel('topic', 'channel')
        conn.publish('topic', b'sup')
        assert conn.stats()['topics'][0]['channels'][0]['depth'] == 1

        conn.empty_channel('topic', 'channel')
        assert conn.stats()['topics'][0]['channels'][0]['depth'] == 0

        if server.version < (0, 3, 6):
            return

        conn.publish('topic', b'sup', 60 * 1000)
        stats = conn.stats()
        assert stats['topics'][0]['channels'][0]['depth'] == 0
        assert stats['topics'][0]['channels'][0]['deferred_count'] == 1
