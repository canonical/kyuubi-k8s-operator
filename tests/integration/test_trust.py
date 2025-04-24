#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import pytest
import yaml
from juju.application import Application
from juju.unit import Unit
from ops import StatusBase
from pytest_operator.plugin import OpsTest

from core.domain import Status

from .helpers import deploy_minimal_kyuubi_setup

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
TEST_CHARM_PATH = "./tests/integration/app-charm"
TEST_CHARM_NAME = "application"


def check_status(entity: Application | Unit, status: StatusBase):
    if isinstance(entity, Application):
        return entity.status == status.name and entity.status_message == status.message
    elif isinstance(entity, Unit):
        return (
            entity.workload_status == status.name
            and entity.workload_status_message == status.message
        )
    else:
        raise ValueError(f"entity type {type(entity)} is not allowed")


@pytest.mark.abort_on_fail
async def test_build_and_deploy(
    ops_test: OpsTest, kyuubi_charm, charm_versions, s3_bucket_and_creds
):
    await deploy_minimal_kyuubi_setup(
        ops_test=ops_test,
        kyuubi_charm=kyuubi_charm,
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=False,
    )
    logger.info(f"State of kyuubi-k8s app: {ops_test.model.applications[APP_NAME].status}")

    # Wait for Kyuubi to settle down to blocked state
    logger.info("Waiting for kyuubi-k8s charm to settle down...")
    await ops_test.model.wait_for_idle(
        apps=[
            APP_NAME,
        ],
        idle_period=20,
        status="blocked",
    )

    # Assert that all charms that were deployed as part of minimal setup are in correct states.
    assert check_status(
        ops_test.model.applications[APP_NAME], Status.INSUFFICIENT_CLUSTER_PERMISSIONS.value
    )
    assert (
        ops_test.model.applications[charm_versions.integration_hub.application_name].status
        == "active"
    )
    assert ops_test.model.applications[charm_versions.s3.application_name].status == "active"


@pytest.mark.abort_on_fail
async def test_provide_clusterwide_trust_permissions(ops_test):
    # Add cluster-wisde trust permission on the application
    await ops_test.juju("trust", APP_NAME, "--scope=cluster")

    await ops_test.model.wait_for_idle(
        apps=[
            APP_NAME,
        ],
        timeout=1000,
        idle_period=20,
    )

    # Assert that the state of Kyuubi changes to Active
    assert check_status(ops_test.model.applications[APP_NAME], Status.ACTIVE.value)
