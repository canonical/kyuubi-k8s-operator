#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import dataclasses
import json
import os
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
import yaml
from charms.tls_certificates_interface.v4.tls_certificates import (
    Certificate,
    CertificateAvailableEvent,
    CertificateSigningRequest,
    PrivateKey,
    ProviderCertificate,
    generate_ca,
    generate_certificate,
    generate_csr,
    generate_private_key,
)
from ops.testing import Context, PeerRelation, Relation, Secret, State

from charm import KyuubiCharm
from constants import KYUUBI_CLIENT_RELATION_NAME, PEER_REL, TLS_REL
from managers.service import Endpoint

PEER = "kyuubi-peers"
CERTS_REL_NAME = "certificates"
TLS_NAME = "self-signed-certificates"
CONFIG = yaml.safe_load(Path("./config.yaml").read_text())
ACTIONS = yaml.safe_load(Path("./actions.yaml").read_text())
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
KYUUBI_PROPERTIES = "/opt/kyuubi/conf/kyuubi-defaults.conf"
KYUUBI_TLS_CONF_DIR = "/opt/kyuubi/conf/"


def parse_kyuubi_configurations(tmp_path: Path) -> dict[str, str]:
    """Parse and return Kyuubi configurations from the conf file in the container."""
    file_path = tmp_path / Path(KYUUBI_PROPERTIES).relative_to("/opt")
    with file_path.open("r") as fid:
        return dict(
            row.rsplit("=", maxsplit=1) for line in fid.readlines() if (row := line.strip())
        )


def validate_file_contents(test_path, file_path, file_content):
    """Validate the given file path contains the given file content."""
    path = test_path / Path(file_path).relative_to("/opt")
    if not os.path.exists(path):
        return False
    with open(path) as f:
        return file_content == f.read()


@pytest.fixture()
def ctx() -> Context:
    ctx = Context(KyuubiCharm, meta=METADATA, config=CONFIG, actions=ACTIONS, unit_id=0)
    return ctx


@pytest.fixture()
def charm_configuration():
    """Enable direct mutation on configuration dict."""
    return json.loads(json.dumps(CONFIG))


@pytest.fixture
def base_state(kyuubi_container) -> State:
    return State(containers=[kyuubi_container], leader=True)


@dataclasses.dataclass
class CertificateAvailableContext:
    # manager: Manager
    context: Context
    peer_relation: PeerRelation
    tls_relation: Relation
    kyuubi_provider_relation: Relation
    state_in: State
    provider_private_key: PrivateKey
    provider_ca_certificate: Certificate
    requirer_private_key: PrivateKey
    client_csr: CertificateSigningRequest
    client_certificate: Certificate
    client_provider_certificate: ProviderCertificate


@pytest.fixture
def certificate_available_context(kyuubi_container, ctx):
    """Create a context for testing certificate available event."""
    context = ctx
    peer_relation = PeerRelation(
        endpoint=PEER_REL,
        local_app_data={"tls": "enabled"},
        local_unit_data={},
        peers_data={},
    )
    client_tls_relation = Relation(endpoint=TLS_REL)
    kyuubi_provider_relation = Relation(
        endpoint=KYUUBI_CLIENT_RELATION_NAME, remote_app_data={"database": "db1"}
    )

    state_in = State(
        relations=[peer_relation, client_tls_relation, kyuubi_provider_relation],
        containers=[kyuubi_container],
    )

    provider_private_key = generate_private_key()
    provider_ca_certificate = generate_ca(
        private_key=provider_private_key,
        common_name="example.com",
        validity=timedelta(days=365),
    )

    requirer_private_key = generate_private_key()
    client_csr = generate_csr(
        private_key=requirer_private_key,
        common_name="etcd-test-1",
    )
    client_certificate = generate_certificate(
        ca_private_key=provider_private_key,
        csr=client_csr,
        ca=provider_ca_certificate,
        validity=timedelta(days=1),
    )
    client_provider_certificate = ProviderCertificate(
        relation_id=client_tls_relation.id,
        certificate=client_certificate,
        certificate_signing_request=client_csr,
        ca=provider_ca_certificate,
        chain=[provider_ca_certificate, client_certificate],
        revoked=False,
    )

    return CertificateAvailableContext(
        context=context,
        peer_relation=peer_relation,
        tls_relation=client_tls_relation,
        kyuubi_provider_relation=kyuubi_provider_relation,
        state_in=state_in,
        provider_private_key=provider_private_key,
        provider_ca_certificate=provider_ca_certificate,
        requirer_private_key=requirer_private_key,
        client_csr=client_csr,
        client_certificate=client_certificate,
        client_provider_certificate=client_provider_certificate,
    )


@pytest.mark.parametrize("is_leader", [True, False])
def test_relation_created_enables_tls(
    is_leader: bool, kyuubi_context: Context[KyuubiCharm], base_state: State
) -> None:
    """Check that relating the charm toggle TLS mode in the databag."""
    # Given
    cluster_peer = PeerRelation(PEER, PEER, local_app_data={}, local_unit_data={}, peers_data={})
    tls_relation = Relation(CERTS_REL_NAME, TLS_NAME)
    state_in: State = dataclasses.replace(
        base_state, relations=[cluster_peer, tls_relation], leader=is_leader
    )

    # When
    state_out = kyuubi_context.run(kyuubi_context.on.relation_created(tls_relation), state_in)

    # Then
    assert state_out.get_relation(cluster_peer.id).local_app_data.get("tls", "") == (
        "enabled" if is_leader else ""
    )


def test_certificate_available_gets_deferred_when_workload_not_ready(
    certificate_available_context,
):
    """Test CertificateAvailble event when workload is not ready."""
    context = certificate_available_context.context
    peer_relation = certificate_available_context.peer_relation
    tls_relation = certificate_available_context.tls_relation
    requirer_private_key = certificate_available_context.requirer_private_key
    provider_ca_certificate = certificate_available_context.provider_ca_certificate
    client_provider_certificate = certificate_available_context.client_provider_certificate
    client_certificate = certificate_available_context.client_certificate
    state_in = certificate_available_context.state_in

    with (
        context(context.on.relation_created(tls_relation), state=state_in) as manager,
        patch(
            "charms.tls_certificates_interface.v4.tls_certificates.TLSCertificatesRequiresV4.get_assigned_certificates",
            return_value=([client_provider_certificate], requirer_private_key),
        ),
        patch("core.workload.kyuubi.KyuubiWorkload.ready", return_value=False),
    ):
        charm: KyuubiCharm = manager.charm
        certificate_available_event = MagicMock(spec=CertificateAvailableEvent)
        state_out = manager.run()
        peer_relation = state_out.get_relation(peer_relation.id)
        certificate_available_event.certificate = client_certificate
        certificate_available_event.ca = provider_ca_certificate
        certificate_available_event.defer = MagicMock()
        charm.tls_events._on_certificate_available(certificate_available_event)

        certificate_available_event.defer.assert_called_once()


@pytest.mark.parametrize(
    "endpoint_host,endpoint_port",
    [("kyuubi-k8s-service.spark.svc.cluster.local", 10009), ("10.1.1.1", 10009)],
)
@pytest.mark.parametrize("tls_client_private_key", [None, generate_private_key()])
@pytest.mark.parametrize("is_leader", [True, False])
def test_certificate_available(
    certificate_available_context,
    tmp_path,
    is_leader,
    tls_client_private_key,
    endpoint_host,
    endpoint_port,
):
    """Test CertificateAvailble event under various scenarios."""
    context = certificate_available_context.context
    tls_relation = certificate_available_context.tls_relation
    kyuubi_provider_relation = certificate_available_context.kyuubi_provider_relation
    provider_ca_certificate = certificate_available_context.provider_ca_certificate
    provider_private_key = certificate_available_context.provider_private_key

    state_in = dataclasses.replace(certificate_available_context.state_in, leader=is_leader)
    if tls_client_private_key:
        private_key_secret = Secret(tracked_content={"private-key": tls_client_private_key.raw})
        state_in = dataclasses.replace(
            state_in,
            config={"tls-client-private-key": private_key_secret.id},
            secrets=[private_key_secret],
        )
        requirer_private_key = tls_client_private_key
        client_csr = generate_csr(
            private_key=requirer_private_key,
            common_name="etcd-test-1",
        )
        client_certificate = generate_certificate(
            ca_private_key=provider_private_key,
            csr=client_csr,
            ca=provider_ca_certificate,
            validity=timedelta(days=1),
        )
        client_provider_certificate = ProviderCertificate(
            relation_id=tls_relation.id,
            certificate=client_certificate,
            certificate_signing_request=client_csr,
            ca=provider_ca_certificate,
            chain=[provider_ca_certificate, client_certificate],
            revoked=False,
        )
    else:
        requirer_private_key = certificate_available_context.requirer_private_key
        client_csr = certificate_available_context.client_csr
        client_certificate = certificate_available_context.client_certificate
        client_provider_certificate = certificate_available_context.client_provider_certificate

    with (
        context(context.on.relation_created(tls_relation), state=state_in) as manager,
        patch(
            "charms.tls_certificates_interface.v4.tls_certificates.TLSCertificatesRequiresV4.get_assigned_certificates",
            return_value=([client_provider_certificate], requirer_private_key),
        ),
        patch(
            "charms.tls_certificates_interface.v4.tls_certificates.TLSCertificatesRequiresV4.get_assigned_certificate",
            return_value=(client_provider_certificate, requirer_private_key),
        ),
        patch("managers.tls.TLSManager.set_truststore") as mock_set_truststore,
        patch("managers.tls.TLSManager.set_p12_keystore") as mock_set_keystore,
        patch(
            "core.workload.kyuubi.KyuubiWorkload.kyuubi_version",
            return_value="1.10.0",
            new_callable=PropertyMock,
        ),
        patch(
            "managers.service.ServiceManager.get_service_endpoint",
            return_value=Endpoint(host=endpoint_host, port=endpoint_port),
        ),
        patch(
            "config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master"
        ),
    ):
        charm: KyuubiCharm = manager.charm
        state_out = manager.run()

        certificate_available_event = MagicMock(spec=CertificateAvailableEvent)
        certificate_available_event.certificate = client_certificate
        certificate_available_event.ca = provider_ca_certificate
        certificate_available_event.certificate_signing_request = client_csr
        certificate_available_event.chain = client_provider_certificate.chain

        # When
        charm.tls_events._on_certificate_available(certificate_available_event)

        # Then

        # Ensure that the keystore and truststore passwords are generated
        assert charm.context.unit_server.truststore_password is not None
        assert charm.context.unit_server.keystore_password is not None

        # Ensure that the CA certificate, server certificate and the private key are in unit peer databag
        assert charm.context.unit_server.certificate == client_certificate.raw
        assert charm.context.unit_server.ca_cert == provider_ca_certificate.raw
        assert charm.context.unit_server.private_key == requirer_private_key.raw
        if tls_client_private_key:
            assert charm.context.unit_server.private_key == tls_client_private_key.raw

        # Ensure that Kyuubi configurations for TLS have been applied to the workload
        kyuubi_config = parse_kyuubi_configurations(tmp_path=tmp_path)
        assert (
            kyuubi_config["kyuubi.frontend.ssl.keystore.password"]
            == charm.context.unit_server.keystore_password
        )
        assert kyuubi_config["kyuubi.frontend.ssl.keystore.path"] == charm.workload.paths.keystore
        assert kyuubi_config["kyuubi.frontend.ssl.keystore.type"] == "PKCS12"
        assert kyuubi_config["kyuubi.frontend.thrift.binary.ssl.enabled"] == "true"
        assert (
            kyuubi_config["kyuubi.frontend.thrift.http.ssl.keystore.password"]
            == charm.context.unit_server.keystore_password
        )
        assert (
            kyuubi_config["kyuubi.frontend.thrift.http.ssl.keystore.path"]
            == charm.workload.paths.keystore
        )
        assert kyuubi_config["kyuubi.frontend.thrift.http.use.SSL"] == "true"

        # Ensure that TLS files have been generated in the container
        assert validate_file_contents(
            test_path=tmp_path,
            file_path=charm.workload.paths.server_key,
            file_content=requirer_private_key.raw,
        )
        assert validate_file_contents(
            test_path=tmp_path,
            file_path=charm.workload.paths.ca,
            file_content=provider_ca_certificate.raw,
        )
        assert validate_file_contents(
            test_path=tmp_path,
            file_path=charm.workload.paths.certificate,
            file_content=client_certificate.raw,
        )
        mock_set_truststore.assert_called_once()
        mock_set_keystore.assert_called_once()

        # Ensure that the Kyuubi provider relation is updated with new TLS data
        kyuubi_provider_relation = state_out.get_relation(kyuubi_provider_relation.id)
        provider_data = kyuubi_provider_relation.local_app_data
        if is_leader:
            assert provider_data["endpoints"] == f"{endpoint_host}:{endpoint_port}"
            assert provider_data["uris"] == f"jdbc:hive2://{endpoint_host}:{endpoint_port}/"
            assert provider_data["tls"] == "True"
            assert provider_data["tls-ca"] == provider_ca_certificate.raw


@pytest.mark.parametrize("is_leader", [True, False])
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch(
    "core.workload.kyuubi.KyuubiWorkload.kyuubi_version",
    return_value="1.10.0",
    new_callable=PropertyMock,
)
@patch(
    "managers.service.ServiceManager.get_service_endpoint",
    return_value=Endpoint(host="example.com", port=10009),
)
@patch("managers.tls.TLSManager.remove_stores")
def test_relation_broken(
    mock_remove_stores,
    mock_service_endpoint,
    mock_kyuubi_version,
    mock_spark_master,
    is_leader: bool,
    kyuubi_context: Context[KyuubiCharm],
    base_state: State,
    tmp_path,
):
    """Upon TLS relation broken, both leader and non-leader unit should cleanup TLS related files."""
    # Given
    cluster_peer = PeerRelation(PEER, PEER, local_app_data={"tls": "enabled"}, peers_data={})
    tls_relation = Relation(CERTS_REL_NAME, TLS_NAME)
    jdbc_relation = Relation(KYUUBI_CLIENT_RELATION_NAME, remote_app_data={"database": "db"})
    state_in: State = dataclasses.replace(
        base_state, relations=[cluster_peer, tls_relation, jdbc_relation], leader=is_leader
    )

    # When
    state_out = kyuubi_context.run(kyuubi_context.on.relation_broken(tls_relation), state_in)

    # Then

    # The removal of TLS files should get triggered for all units
    mock_remove_stores.assert_called_once()

    # TLS related configurations should be removed for all units
    kyuubi_config = parse_kyuubi_configurations(tmp_path=tmp_path)
    assert "kyuubi.frontend.ssl.keystore.password" not in kyuubi_config
    assert "kyuubi.frontend.ssl.keystore.path" not in kyuubi_config
    assert "kyuubi.frontend.ssl.keystore.type" not in kyuubi_config
    assert "kyuubi.frontend.thrift.binary.ssl.enabled" not in kyuubi_config
    assert "kyuubi.frontend.thrift.http.ssl.keystore.password" not in kyuubi_config
    assert "kyuubi.frontend.thrift.http.ssl.keystore.path" not in kyuubi_config
    assert "kyuubi.frontend.thrift.http.use.SSL" not in kyuubi_config

    # The peer app data should not have "tls": "enabled" (leader should do this)
    if is_leader:
        peer_app_data = state_out.get_relation(cluster_peer.id).local_app_data
        assert peer_app_data.get("tls", "") == ("" if is_leader else "enabled")

        jdbc_app_data = state_out.get_relation(jdbc_relation.id).local_app_data
        assert jdbc_app_data["tls"] == "False"
        assert jdbc_app_data.get("tls-ca", "") == ""
