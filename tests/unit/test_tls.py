#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import dataclasses
import json
from pathlib import Path

import yaml
from ops.testing import Context, PeerRelation, Relation, State
from pytest import fixture

from charm import KyuubiCharm

PEER = "kyuubi-peers"
CERTS_REL_NAME = "certificates"
TLS_NAME = "self-signed-certificates"
CONFIG = yaml.safe_load(Path("./config.yaml").read_text())
ACTIONS = yaml.safe_load(Path("./actions.yaml").read_text())
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())


@fixture()
def ctx() -> Context:
    ctx = Context(KyuubiCharm, meta=METADATA, config=CONFIG, actions=ACTIONS, unit_id=0)
    return ctx


@fixture()
def charm_configuration():
    """Enable direct mutation on configuration dict."""
    return json.loads(json.dumps(CONFIG))


@fixture
def base_state(kyuubi_container) -> State:
    return State(containers=[kyuubi_container], leader=True)


def test_relation_created_enables_tls(
    kyuubi_context: Context[KyuubiCharm], base_state: State
) -> None:
    """Check that relating the charm toggle TLS mode in the databag."""
    # Given
    cluster_peer = PeerRelation(PEER, PEER, local_app_data={}, local_unit_data={}, peers_data={})
    tls_relation = Relation(CERTS_REL_NAME, TLS_NAME)
    state_in: State = dataclasses.replace(base_state, relations=[cluster_peer, tls_relation])

    # When
    state_out = kyuubi_context.run(kyuubi_context.on.relation_created(tls_relation), state_in)

    # Then
    assert state_out.get_relation(cluster_peer.id).local_app_data.get("tls", "") == "enabled"


# def test_relation_joined_tls_disabled_defers(
#     kyuubi_context: Context[KyuubiCharm], base_state: State
# ) -> None:
#     """Check that joining the relation before toggling TLS mode defers the event."""
#     # Given
#     cluster_peer = PeerRelation(PEER, PEER, local_app_data={}, local_unit_data={}, peers_data={})
#     tls_relation = Relation(CERTS_REL_NAME, TLS_NAME)
#     state_in: State = dataclasses.replace(base_state, relations=[cluster_peer, tls_relation])

#     # When
#     with (
#         patch("ops.framework.EventBase.defer") as patched,
#         kyuubi_context(kyuubi_context.on.relation_joined(tls_relation), state_in) as manager,
#     ):
#         manager.run()

#         # Then
#         patched.assert_called_once()
#         assert not manager.charm.context.unit_server.private_key


# def test_relation_joined_request_tls_cert_not_exposed(
#     kyuubi_context: Context[KyuubiCharm], base_state: State
# ) -> None:
#     """Check that joining the relation creates a private unit key and requests a cert."""
#     # Given
#     ctx = kyuubi_context
#     cluster_peer = PeerRelation(
#         PEER,
#         PEER,
#         local_app_data={"tls": "enabled"},
#         peers_data={},
#     )
#     tls_relation = Relation(CERTS_REL_NAME, TLS_NAME)
#     state_in: State = dataclasses.replace(base_state, relations=[cluster_peer, tls_relation])

#     # When
#     with ctx(ctx.on.relation_joined(tls_relation), state_in) as manager:
#         state_out = manager.run()

#         # Then
#         assert manager.charm.context.unit_server.private_key
#         assert (
#             "BEGIN RSA PRIVATE KEY"
#             in manager.charm.context.unit_server.private_key.splitlines()[0]
#         )
#         assert manager.charm.context.unit_server.csr

#     assert state_out.get_relation(tls_relation.id).local_unit_data.get(
#         "certificate_signing_requests", ""
#     )


# def test_relation_joined_request_tls_cert_loadbalancer(
#     charm_configuration: dict, base_state: State
# ) -> None:
#     """Check that joining the relation creates a private unit key and requests a cert.

#     We assume that the loadbalancer will use a hostname instead of an IP address.
#     """
#     # Given
#     charm_configuration["options"]["expose-external"]["default"] = "loadbalancer"
#     ctx = Context(
#         KyuubiCharm, meta=METADATA, config=charm_configuration, actions=ACTIONS, unit_id=0
#     )

#     cluster_peer = PeerRelation(
#         PEER, PEER, local_app_data={"tls": "enabled"}, local_unit_data={}, peers_data={}
#     )
#     tls_relation = Relation(CERTS_REL_NAME, TLS_NAME)
#     state_in: State = dataclasses.replace(base_state, relations=[cluster_peer, tls_relation])
#     expected_endpoint = "rincewind.discworld"

#     # When
#     with (
#         patch(
#             "core.domain.KyuubiServer.external_address",
#             new_callable=PropertyMock,
#             return_value=DNSEndpoint(expected_endpoint, JDBC_PORT),
#         ),
#         ctx(ctx.on.relation_joined(tls_relation), state_in) as manager,
#     ):
#         state_out = manager.run()

#         # Then
#         assert manager.charm.context.unit_server.private_key
#         assert (
#             "BEGIN RSA PRIVATE KEY"
#             in manager.charm.context.unit_server.private_key.splitlines()[0]
#         )
#         assert manager.charm.context.unit_server.csr

#     assert (
#         csrs := state_out.get_relation(tls_relation.id).local_unit_data.get(
#             "certificate_signing_requests", ""
#         )
#     )
#     csr_pem = json.loads(csrs)[0]["certificate_signing_request"]

#     csr = load_pem_x509_csr(csr_pem.encode("utf-8"), default_backend())
#     assert any(
#         cn.value == expected_endpoint
#         for cn in csr.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
#     )
