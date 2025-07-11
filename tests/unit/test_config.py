#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import json
import logging
from pathlib import Path
from unittest.mock import patch

import ops
import pytest
import yaml
from ops.testing import Container, Context, PeerRelation, Relation, State

from charm import KyuubiCharm
from constants import KYUUBI_CONTAINER_NAME
from core.domain import Status
from managers.service import Endpoint

from .helpers import (
    parse_spark_properties,
)

logger = logging.getLogger(__name__)


CONFIG = yaml.safe_load(Path("./config.yaml").read_text())
ACTIONS = yaml.safe_load(Path("./actions.yaml").read_text())
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())


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


# def check_invalid_values(field: str, erroneus_values: Iterable) -> None:
#     """Check the incorrectness of the passed values for a field."""
#     flat_config_options = {
#         option_name: mapping.get("default") for option_name, mapping in CONFIG["options"].items()
#     }
#     for value in erroneus_values:
#         with pytest.raises(ValidationError) as excinfo:
#             CharmConfig(**{**flat_config_options, **{field: value}})
#         assert field in excinfo.value.errors()[0]["loc"]


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
    return_value=Endpoint(host="10.10.10.10", port=10009),
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
@pytest.mark.parametrize("profile", ["testing", "production"])
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
    profile,
) -> None:
    """Test profile config option."""
    state = State(
        relations=[spark_service_account_relation, auth_db_relation, kyuubi_peers_relation],
        containers=[kyuubi_container],
        config={"profile": profile},
        leader=True,
    )
    out = kyuubi_context.run(kyuubi_context.on.config_changed(), state)
    assert out.unit_status == Status.ACTIVE.value

    spark_properties = parse_spark_properties(tmp_path)
    logger.info(f"profile: testing => spark_properties: {spark_properties}")
    if profile == "testing":
        assert "spark.kubernetes.executor.request.cores" in spark_properties
        assert spark_properties["spark.kubernetes.executor.request.cores"] == "100m"
        assert spark_properties["spark.kubernetes.driver.request.cores"] == "100m"
    else:
        assert "spark.kubernetes.executor.request.cores" not in spark_properties
        assert "spark.kubernetes.driver.request.cores" not in spark_properties


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
@pytest.mark.parametrize(
    "k8s_selectors",
    [
        ("a:b", [("spark.kubernetes.node.selector.a", "b")]),
        (
            "a:b,c:d",
            [("spark.kubernetes.node.selector.a", "b"), ("spark.kubernetes.node.selector.c", "d")],
        ),
    ],
)
def test_k8s_node_selectors_config_option(
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
    k8s_selectors,
) -> None:
    """Test profile config option."""
    logger.info(f"{k8s_selectors[0]}")
    logger.info(f"{k8s_selectors[1]}")
    state = State(
        relations=[spark_service_account_relation, auth_db_relation, kyuubi_peers_relation],
        containers=[kyuubi_container],
        config={"k8s-node-selectors": k8s_selectors[0]},
        leader=True,
    )
    out = kyuubi_context.run(kyuubi_context.on.config_changed(), state)
    assert out.unit_status == Status.ACTIVE.value

    spark_properties = parse_spark_properties(tmp_path)
    logger.info(f"profile: testing => spark_properties: {spark_properties}")

    for k, v in k8s_selectors[1]:
        assert k in spark_properties
        assert spark_properties[k] == v


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
@pytest.mark.parametrize(
    "k8s_selectors",
    [
        ("ab", [("spark.kubernetes.node.selector.a", "b")]),
        (
            "a:b,c:d,a:c",
            [("spark.kubernetes.node.selector.a", "b"), ("spark.kubernetes.node.selector.c", "d")],
        ),
    ],
)
def test_wrong_k8s_node_selectors_config_option(
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
    k8s_selectors,
) -> None:
    """Test profile config option."""
    state = State(
        relations=[spark_service_account_relation, auth_db_relation, kyuubi_peers_relation],
        containers=[kyuubi_container],
        config={"k8s-node-selectors": k8s_selectors[0]},
        leader=True,
    )
    try:
        _ = kyuubi_context.run(kyuubi_context.on.config_changed(), state)
    except ops.testing.errors.UncaughtCharmError as excinfo:
        assert "ValidationError" in str(excinfo)
