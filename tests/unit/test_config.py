#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import json
import logging
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from ops import testing
from ops.testing import Container, Context, Relation, State

from charm import KyuubiCharm
from constants import KYUUBI_CONTAINER_NAME

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


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
@patch(
    "managers.service.ServiceManager.get_service_endpoint",
    return_value="10.10.10.10:10009",
)
@patch("managers.service.ServiceManager.reconcile_services", return_value=True)
@patch("events.provider.KyuubiClientProviderEvents.update_clients_endpoints", return_value=None)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch(
    "config.spark.SparkConfig._sa_conf",
    return_value={
        "new_property": "new_value",
        "spark.kubernetes.container.image": "image_from_service_account",
    },
)
def test_profile_config_option(
    mock_sa_conf,
    mock_get_master,
    mock_get_service_endpoint,
    mock_reconcile_services,
    mock_update_clients_endpoints,
    mock_s3_configured,
    mock_valid_sa,
    mock_valid_ns,
    tmp_path,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    auth_db_relation: Relation,
    charm_configuration: dict,
):
    """Checks that the profile config option is set correctly."""
    # Given
    charm_configuration["options"]["profile"]["default"] = "testing"
    ctx = Context(
        KyuubiCharm, meta=METADATA, config=charm_configuration, actions=ACTIONS, unit_id=0
    )

    state_in = testing.State(
        config={"profile": "testing"},
        relations={spark_service_account_relation, auth_db_relation},
        leader=True,
        containers=[kyuubi_container],
    )
    _ = kyuubi_context.run(ctx.on.config_changed(), state_in)

    spark_properties = parse_spark_properties(tmp_path)
    logger.info(f"profile: testing => spark_properties: {spark_properties}")

    assert "spark.kubernetes.executor.request.cores" in spark_properties

    state_in = testing.State(
        config={"profile": "production"},
        relations={spark_service_account_relation, auth_db_relation},
        leader=True,
        containers=[kyuubi_container],
    )
    _ = kyuubi_context.run(ctx.on.config_changed(), state_in)

    spark_properties = parse_spark_properties(tmp_path)
    logger.info(f"profile: production => spark_properties: {spark_properties}")

    assert "spark.kubernetes.executor.request.cores" not in spark_properties
