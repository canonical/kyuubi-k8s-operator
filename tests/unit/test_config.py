#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import json
import logging
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from ops.testing import Container, Context, PeerRelation, Relation, Secret, State

from charm import KyuubiCharm
from constants import KYUUBI_CONTAINER_NAME
from core.domain import Status

logger = logging.getLogger(__name__)


CONFIG = yaml.safe_load(Path("./config.yaml").read_text())
ACTIONS = yaml.safe_load(Path("./actions.yaml").read_text())
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())

SPARK_PROPERTIES = "/etc/spark8t/conf/spark-defaults.conf"


def parse_spark_properties(tmp_path: Path) -> dict[str, str]:
    """Parse and return spark properties from the conf file in the container."""
    file_path = tmp_path / Path(SPARK_PROPERTIES).relative_to("/etc")
    with file_path.open("r") as fid:
        return dict(
            row.rsplit("=", maxsplit=1) for line in fid.readlines() if (row := line.strip())
        )


@pytest.fixture()
def base_state():
    return State(leader=True, containers=[Container(name=KYUUBI_CONTAINER_NAME, can_connect=True)])


@pytest.fixture()
def ctx() -> Context:
    ctx = Context(
        KyuubiCharm,
        meta=METADATA,
        config=CONFIG,
        actions=ACTIONS,
    )
    return ctx


@pytest.fixture()
def charm_configuration():
    """Enable direct mutation on configuration dict."""
    return json.loads(json.dumps(CONFIG))


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
    return_value="10.10.10.10:10009",
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
def test_profile_config_option(
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
    tmp_path,
) -> None:
    """Test when the admin password supplied via system-users config option is valid."""
    system_users_secret = Secret(tracked_content={"admin": "password"})
    state = State(
        relations=[spark_service_account_relation, auth_db_relation, kyuubi_peers_relation],
        containers=[kyuubi_container],
        config={"system-users": system_users_secret.id, "profile": "testing"},
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

    spark_properties = parse_spark_properties(tmp_path)
    logger.info(f"profile: testing => spark_properties: {spark_properties}")
    assert "spark.kubernetes.executor.request.cores" in spark_properties

    state_in = State(
        relations=[spark_service_account_relation, auth_db_relation, kyuubi_peers_relation],
        containers=[kyuubi_container],
        config={"profile": "production"},
        secrets=[system_users_secret],
        leader=True,
    )
    out = kyuubi_context.run(kyuubi_context.on.config_changed(), state_in)
    assert out.unit_status == Status.ACTIVE.value
    spark_properties = parse_spark_properties(tmp_path)
    logger.info(f"profile: production => spark_properties: {spark_properties}")

    assert "spark.kubernetes.executor.request.cores" not in spark_properties
