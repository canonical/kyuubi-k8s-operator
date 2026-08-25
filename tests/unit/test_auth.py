#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import json
import logging
from pathlib import Path
from unittest.mock import patch

from ops.testing import Container, Context, PeerRelation, Relation, Secret, State

from constants import LDAP_RELATION_NAME
from core.domain import Status
from managers.service import Endpoint

logger = logging.getLogger(__name__)

KYUUBI_PROPERTIES = "/opt/kyuubi/conf/kyuubi-defaults.conf"

LDAP_BASE_DN = "dc=glauth,dc=com"
LDAP_BIND_DN = "cn=serviceuser,ou=svcaccts,dc=glauth,dc=com"
LDAP_BIND_PASSWORD = "bind-password"
LDAP_URLS = ["ldap://glauth-k8s.test:3893"]
LDAPS_URLS = ["ldaps://glauth-k8s.test:6360"]


def parse_kyuubi_configurations(tmp_path: Path) -> dict[str, str]:
    """Parse and return Kyuubi configurations from the conf file in the container."""
    file_path = tmp_path / Path(KYUUBI_PROPERTIES).relative_to("/opt")
    with file_path.open("r") as fid:
        return dict(
            row.split("=", maxsplit=1) for line in fid.readlines() if (row := line.strip())
        )


def build_ldap_secret() -> Secret:
    """Provide a remote-owned secret carrying the LDAP bind password."""
    return Secret(tracked_content={"password": LDAP_BIND_PASSWORD})


def build_ldap_relation(bind_password_secret_id: str, ldaps_urls: list[str] = LDAPS_URLS):
    """Build an LDAP relation as populated by the glauth provider charm."""
    return Relation(
        endpoint=LDAP_RELATION_NAME,
        interface="ldap",
        remote_app_name="glauth-k8s",
        remote_app_data={
            "urls": json.dumps(LDAP_URLS),
            "ldaps_urls": json.dumps(ldaps_urls),
            "base_dn": LDAP_BASE_DN,
            "bind_dn": LDAP_BIND_DN,
            "bind_password_secret": bind_password_secret_id,
            "auth_method": "simple",
            "starttls": "True",
        },
    )


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("managers.integration_hub.IntegrationHubManager.is_s3_configured", return_value=True)
@patch("config.spark.SparkConfig._sa_conf", return_value={})
def test_system_users_config_secret_doesnot_exist(
    mock_sa_conf,
    mock_s3_configured,
    mock_get_master,
    mock_valid_sa,
    mock_valid_ns,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    auth_db_relation: Relation,
) -> None:
    """Test when the admin password supplied via system-users config option does not exist."""
    system_users_secret = Secret(tracked_content={"admin": "password"})
    state = State(
        relations=[spark_service_account_relation, auth_db_relation],
        containers=[kyuubi_container],
        config={"system-users": system_users_secret.id},
    )
    out = kyuubi_context.run(kyuubi_context.on.config_changed(), state)
    assert out.unit_status == Status.SYSTEM_USERS_SECRET_DOES_NOT_EXIST.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("managers.integration_hub.IntegrationHubManager.is_s3_configured", return_value=True)
@patch("config.spark.SparkConfig._sa_conf", return_value={})
def test_system_users_config_secret_invalid(
    mock_sa_conf,
    mock_s3_configured,
    mock_get_master,
    mock_valid_sa,
    mock_valid_ns,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    auth_db_relation: Relation,
) -> None:
    """Test when the admin password supplied via system-users config option is invalid."""
    system_users_secret = Secret(tracked_content={"some-user": "password"})
    state = State(
        relations=[spark_service_account_relation, auth_db_relation],
        containers=[kyuubi_container],
        config={"system-users": system_users_secret.id},
        secrets=[system_users_secret],
    )
    out = kyuubi_context.run(kyuubi_context.on.config_changed(), state)
    assert out.unit_status == Status.SYSTEM_USERS_SECRET_INVALID.value


@patch("managers.auth.jdbc.JDBCAuthenticationManager.set_password")
@patch("managers.auth.jdbc.JDBCAuthenticationManager.user_exists", return_value=True)
@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch(
    "events.provider.KyuubiClientProviderEvents.update_clients_endpoints",
    return_value=True,
)
@patch(
    "managers.service.ServiceManager.get_service_endpoint",
    return_value=[Endpoint(host="10.10.10.10", port=10009)],
)
@patch(
    "managers.service.ServiceManager.reconcile_services",
    return_value=True,
)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("managers.integration_hub.IntegrationHubManager.is_s3_configured", return_value=True)
@patch(
    "config.spark.SparkConfig._sa_conf", return_value={"spark.hadoop.fs.s3a.endpoint": "foo.bar"}
)
def test_system_users_config_secret_valid(
    mock_sa_conf,
    mock_s3_configured,
    mock_get_master,
    mock_reconcile_service,
    mock_service_endpoint,
    mock_update_client_endpoints,
    mock_valid_sa,
    mock_valid_ns,
    mock_user_exists,
    mock_set_password,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    auth_db_relation: Relation,
    kyuubi_peers_relation: PeerRelation,
) -> None:
    """Test when the admin password supplied via system-users config option is valid."""
    system_users_secret = Secret(tracked_content={"admin": "password"})
    state = State(
        relations=[spark_service_account_relation, auth_db_relation, kyuubi_peers_relation],
        containers=[kyuubi_container],
        config={"system-users": system_users_secret.id},
        secrets=[system_users_secret],
        leader=True,
    )
    out = kyuubi_context.run(kyuubi_context.on.config_changed(), state)
    assert out.unit_status == Status.ACTIVE.value

    assert mock_set_password.called
    _, kwargs = mock_set_password.call_args
    assert kwargs["username"] == "admin"
    assert kwargs["password"] == "password"

    peer_app_secret = [
        secret for secret in out.secrets if secret.label == "kyuubi-peers.kyuubi-k8s.app"
    ]
    assert len(peer_app_secret) > 0
    peer_app_secret_content = peer_app_secret[0].latest_content
    assert peer_app_secret_content is not None
    assert peer_app_secret_content["admin-password"] == "password"


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch(
    "events.provider.KyuubiClientProviderEvents.update_clients_endpoints",
    return_value=True,
)
@patch(
    "managers.service.ServiceManager.get_service_endpoint",
    return_value=[Endpoint(host="10.10.10.10", port=10009)],
)
@patch("managers.service.ServiceManager.reconcile_services", return_value=True)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("managers.integration_hub.IntegrationHubManager.is_s3_configured", return_value=True)
@patch(
    "config.spark.SparkConfig._sa_conf", return_value={"spark.hadoop.fs.s3a.endpoint": "foo.bar"}
)
def test_ldap_ready_configures_ldap_authentication(
    mock_sa_conf,
    mock_s3_configured,
    mock_get_master,
    mock_reconcile_service,
    mock_service_endpoint,
    mock_update_client_endpoints,
    mock_valid_sa,
    mock_valid_ns,
    tmp_path,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    kyuubi_peers_relation: PeerRelation,
) -> None:
    """Adding an LDAP relation should enable LDAP authentication in Kyuubi."""
    ldap_secret = build_ldap_secret()
    ldap_relation = build_ldap_relation(ldap_secret.id)
    state = State(
        relations=[spark_service_account_relation, ldap_relation, kyuubi_peers_relation],
        containers=[kyuubi_container],
        secrets=[ldap_secret],
        leader=True,
    )

    out = kyuubi_context.run(kyuubi_context.on.relation_changed(ldap_relation), state)

    assert out.unit_status == Status.ACTIVE.value

    kyuubi_configurations = parse_kyuubi_configurations(tmp_path)
    assert kyuubi_configurations["kyuubi.authentication"] == "LDAP"
    assert kyuubi_configurations["kyuubi.authentication.ldap.baseDN"] == LDAP_BASE_DN
    assert kyuubi_configurations["kyuubi.authentication.ldap.binddn"] == LDAP_BIND_DN
    assert kyuubi_configurations["kyuubi.authentication.ldap.bindpw"] == LDAP_BIND_PASSWORD
    assert kyuubi_configurations["kyuubi.authentication.ldap.url"] == " ".join(LDAPS_URLS)


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.kyuubi.KyuubiManager.update")
def test_ldap_ready_triggers_kyuubi_update(
    mock_update,
    mock_valid_sa,
    mock_valid_ns,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    kyuubi_peers_relation: PeerRelation,
) -> None:
    """The ldap_ready event must be wired to a Kyuubi service update."""
    ldap_secret = build_ldap_secret()
    ldap_relation = build_ldap_relation(ldap_secret.id)
    state = State(
        relations=[spark_service_account_relation, ldap_relation, kyuubi_peers_relation],
        containers=[kyuubi_container],
        secrets=[ldap_secret],
        leader=True,
    )

    kyuubi_context.run(kyuubi_context.on.relation_changed(ldap_relation), state)

    assert mock_update.called
    assert all(not call.kwargs.get("set_ldap_none") for call in mock_update.call_args_list)


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.kyuubi.KyuubiManager.update")
def test_ldap_unavailable_triggers_kyuubi_update_with_ldap_none(
    mock_update,
    mock_valid_sa,
    mock_valid_ns,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    kyuubi_peers_relation: PeerRelation,
) -> None:
    """Removing the LDAP relation must be wired to an update that clears LDAP config."""
    ldap_secret = build_ldap_secret()
    ldap_relation = build_ldap_relation(ldap_secret.id)
    state = State(
        relations=[spark_service_account_relation, ldap_relation, kyuubi_peers_relation],
        containers=[kyuubi_container],
        secrets=[ldap_secret],
        leader=True,
    )

    kyuubi_context.run(kyuubi_context.on.relation_broken(ldap_relation), state)

    assert mock_update.called
    assert any(call.kwargs.get("set_ldap_none") for call in mock_update.call_args_list)


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch(
    "events.provider.KyuubiClientProviderEvents.update_clients_endpoints",
    return_value=True,
)
@patch(
    "managers.service.ServiceManager.get_service_endpoint",
    return_value=[Endpoint(host="10.10.10.10", port=10009)],
)
@patch("managers.service.ServiceManager.reconcile_services", return_value=True)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("managers.integration_hub.IntegrationHubManager.is_s3_configured", return_value=True)
@patch(
    "config.spark.SparkConfig._sa_conf", return_value={"spark.hadoop.fs.s3a.endpoint": "foo.bar"}
)
def test_ldap_relation_broken_removes_ldap_authentication(
    mock_sa_conf,
    mock_s3_configured,
    mock_get_master,
    mock_reconcile_service,
    mock_service_endpoint,
    mock_update_client_endpoints,
    mock_valid_sa,
    mock_valid_ns,
    tmp_path,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    kyuubi_peers_relation: PeerRelation,
) -> None:
    """Removing the LDAP relation should drop LDAP authentication from Kyuubi config."""
    ldap_secret = build_ldap_secret()
    ldap_relation = build_ldap_relation(ldap_secret.id)
    state = State(
        relations=[spark_service_account_relation, ldap_relation, kyuubi_peers_relation],
        containers=[kyuubi_container],
        secrets=[ldap_secret],
        leader=True,
    )

    kyuubi_context.run(kyuubi_context.on.relation_broken(ldap_relation), state)

    kyuubi_configurations = parse_kyuubi_configurations(tmp_path)
    assert "kyuubi.authentication" not in kyuubi_configurations


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch(
    "managers.service.ServiceManager.get_service_endpoint",
    return_value=[Endpoint(host="10.10.10.10", port=10009)],
)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("managers.integration_hub.IntegrationHubManager.is_s3_configured", return_value=True)
@patch("config.spark.SparkConfig._sa_conf", return_value={})
def test_ldap_without_ldaps_urls_is_blocked(
    mock_sa_conf,
    mock_s3_configured,
    mock_get_master,
    mock_service_endpoint,
    mock_valid_sa,
    mock_valid_ns,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    kyuubi_peers_relation: PeerRelation,
) -> None:
    """An LDAP relation without LDAPS URLs must block the charm."""
    ldap_secret = build_ldap_secret()
    ldap_relation = build_ldap_relation(ldap_secret.id, ldaps_urls=[])
    state = State(
        relations=[spark_service_account_relation, ldap_relation, kyuubi_peers_relation],
        containers=[kyuubi_container],
        secrets=[ldap_secret],
        leader=True,
    )

    out = kyuubi_context.run(kyuubi_context.on.relation_changed(ldap_relation), state)

    assert out.unit_status == Status.LDAP_CONNECTION_NOT_SECURE.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch(
    "managers.service.ServiceManager.get_service_endpoint",
    return_value=[Endpoint(host="10.10.10.10", port=10009)],
)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("managers.integration_hub.IntegrationHubManager.is_s3_configured", return_value=True)
@patch("config.spark.SparkConfig._sa_conf", return_value={})
def test_ldap_and_jdbc_auth_relations_are_blocked(
    mock_sa_conf,
    mock_s3_configured,
    mock_get_master,
    mock_service_endpoint,
    mock_valid_sa,
    mock_valid_ns,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    auth_db_relation: Relation,
    kyuubi_peers_relation: PeerRelation,
) -> None:
    """Having both LDAP and JDBC authentication relations must block the charm."""
    ldap_secret = build_ldap_secret()
    ldap_relation = build_ldap_relation(ldap_secret.id)
    state = State(
        relations=[
            spark_service_account_relation,
            auth_db_relation,
            ldap_relation,
            kyuubi_peers_relation,
        ],
        containers=[kyuubi_container],
        secrets=[ldap_secret],
        leader=True,
    )

    out = kyuubi_context.run(kyuubi_context.on.relation_changed(ldap_relation), state)

    assert out.unit_status == Status.MULTIPLE_AUTH_RELATIONS.value
