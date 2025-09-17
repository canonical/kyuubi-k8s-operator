# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.


import dataclasses

import pytest
from ops.testing import Container, Context, PeerRelation, Relation, State

from constants import KYUUBI_CLIENT_RELATION_NAME, PEER_REL


@pytest.fixture
def base_state(kyuubi_container: Container) -> State:
    return State(containers=[kyuubi_container], leader=True)


def test_database_requested_deferred_if_missing_auth_db(
    kyuubi_context: Context,
    base_state: State,
) -> None:
    # Given
    cluster_peer = PeerRelation(PEER_REL, PEER_REL)
    client_relation = Relation(
        endpoint=KYUUBI_CLIENT_RELATION_NAME,
        interface="kyuubi_client",
        remote_app_data={"database": "testdb"},
    )
    state_in = dataclasses.replace(base_state, relations=[cluster_peer, client_relation])
    # When
    # Cannot use custom event, as DatabaseRequested does not inherit directly from EventBase
    state_out = kyuubi_context.run(kyuubi_context.on.relation_changed(client_relation), state_in)
    # Then
    assert len(state_out.deferred) == 1
