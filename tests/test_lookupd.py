from __future__ import with_statement

import pytest
import gnsq
from integration_server import LookupdIntegrationServer


@pytest.mark.slow
def test_basic():
    with LookupdIntegrationServer() as server:
        conn = gnsq.Lookupd(server.http_address)
        assert conn.ping() == 'OK'
        assert 'version' in conn.info()
