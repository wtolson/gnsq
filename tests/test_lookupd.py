from __future__ import with_statement

import pytest
import gevent
import gnsq

from integration_server import (
    LookupdIntegrationServer,
    NsqdIntegrationServer
)


@pytest.mark.slow
@pytest.mark.timeout(60)
def test_basic():
    with LookupdIntegrationServer() as server:
        lookupd = gnsq.Lookupd(server.http_address)
        assert lookupd.ping() == b'OK'
        assert 'version' in lookupd.info()

        with pytest.raises(gnsq.errors.NSQHttpError):
            lookupd.lookup('topic')

        assert len(lookupd.topics()['topics']) == 0
        assert len(lookupd.channels('topic')['channels']) == 0
        assert len(lookupd.nodes()['producers']) == 0


@pytest.mark.slow
@pytest.mark.timeout(60)
def test_lookup():
    with LookupdIntegrationServer() as lookupd_server:
        nsqd_server = NsqdIntegrationServer(lookupd=lookupd_server.tcp_address)
        with NsqdIntegrationServer(lookupd=lookupd_server.tcp_address) as nsqd_server:
            lookupd = gnsq.Lookupd(lookupd_server.http_address)
            conn = gnsq.Nsqd(nsqd_server.address, http_port=nsqd_server.http_port)
            gevent.sleep(0.1)

            assert len(lookupd.topics()['topics']) == 0
            assert len(lookupd.channels('topic')['channels']) == 0
            assert len(lookupd.nodes()['producers']) == 1

            conn.create_topic('topic')
            gevent.sleep(0.1)

            info = lookupd.lookup('topic')
            assert len(info['channels']) == 0
            assert len(info['producers']) == 1
            assert len(lookupd.topics()['topics']) == 1
            assert len(lookupd.channels('topic')['channels']) == 0

            conn.create_channel('topic', 'channel')
            gevent.sleep(0.1)

            info = lookupd.lookup('topic')
            assert len(info['channels']) == 1
            assert len(info['producers']) == 1
            assert len(lookupd.topics()['topics']) == 1
            assert len(lookupd.channels('topic')['channels']) == 1
