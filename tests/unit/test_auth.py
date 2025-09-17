#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
from unittest.mock import patch

from ops.testing import Container, Context, PeerRelation, Relation, Secret, State

from core.domain import Status
from managers.service import Endpoint

logger = logging.getLogger(__name__)


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
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
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
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


@patch("managers.auth.AuthenticationManager.set_password")
@patch("managers.auth.AuthenticationManager.user_exists", return_value=True)
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
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
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
