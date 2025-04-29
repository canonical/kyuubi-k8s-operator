#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import logging
import uuid
from pathlib import Path

import pytest
import yaml
from pytest_operator.plugin import OpsTest
from thrift.transport.TTransport import TTransportException

from .helpers import (
    deploy_minimal_kyuubi_setup,
    find_leader_unit,
    validate_sql_queries_with_kyuubi,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
TEST_CHARM_NAME = "application"
INVALID_METASTORE_APP_NAME = "invalid-metastore"


@pytest.mark.abort_on_fail
async def test_deploy_minimal_kyuubi_setup(
    ops_test: OpsTest,
    kyuubi_charm: Path,
    charm_versions,
    s3_bucket_and_creds,
) -> None:
    """Deploy the minimal setup for Kyuubi and assert all charms are in active and idle state."""
    await deploy_minimal_kyuubi_setup(
        ops_test=ops_test,
        kyuubi_charm=kyuubi_charm,
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=True,
    )

    # Wait for everything to settle down
    await ops_test.model.wait_for_idle(
        apps=[
            APP_NAME,
            charm_versions.integration_hub.application_name,
            charm_versions.s3.application_name,
            charm_versions.auth_db.application_name,
        ],
        idle_period=20,
        status="active",
    )


@pytest.mark.abort_on_fail
async def test_kyuubi_without_passing_credentials(ops_test: OpsTest):
    """Test the JDBC connection when invalid credentials are provided."""
    with pytest.raises(TTransportException) as exc:
        await validate_sql_queries_with_kyuubi(ops_test=ops_test)
        assert b"Error validating the login" in exc.value.message


@pytest.mark.abort_on_fail
async def test_kyuubi_with_invalid_credentials(ops_test: OpsTest):
    """Test the JDBC connection when invalid credentials are provided."""
    username = "admin"
    password = str(uuid.uuid4())
    with pytest.raises(TTransportException) as exc:
        await validate_sql_queries_with_kyuubi(
            ops_test=ops_test, username=username, password=password
        )
        assert b"Error validating the login" in exc.value.message


@pytest.mark.abort_on_fail
async def test_kyuubi_valid_credentials(ops_test: OpsTest):
    """Test the JDBC connection when invalid credentials are provided."""
    kyuubi_leader = await find_leader_unit(ops_test, app_name=APP_NAME)
    assert kyuubi_leader is not None

    logger.info("Running action 'get-password' on kyuubi-k8s unit...")
    action = await kyuubi_leader.run_action(
        action_name="get-password",
    )
    result = await action.wait()

    password = result.results.get("password")
    logger.info(f"Fetched password: {password}")

    username = "admin"
    assert await validate_sql_queries_with_kyuubi(
        ops_test=ops_test, username=username, password=password
    )


@pytest.mark.abort_on_fail
async def test_set_password_action(ops_test: OpsTest):
    """Test set-password action."""
    logger.info("Running action 'get-password' on kyuubi-k8s unit...")
    kyuubi_unit = await find_leader_unit(ops_test, app_name=APP_NAME)
    assert kyuubi_unit is not None
    action = await kyuubi_unit.run_action(
        action_name="get-password",
    )
    result = await action.wait()
    old_password = result.results.get("password")

    logger.info("Running action 'set-password' on kyuubi-k8s unit...")
    password_to_set = str(uuid.uuid4())
    action = await kyuubi_unit.run_action(action_name="set-password", password=password_to_set)
    result = await action.wait()
    assert result.results.get("password") == password_to_set

    logger.info("Running action 'get-password' on kyuubi-k8s unit...")
    action = await kyuubi_unit.run_action(
        action_name="get-password",
    )
    result = await action.wait()
    new_password = result.results.get("password")

    assert new_password != old_password
    assert new_password == password_to_set

    username = "admin"
    assert await validate_sql_queries_with_kyuubi(
        ops_test=ops_test, username=username, password=new_password
    )


@pytest.mark.abort_on_fail
async def test_remove_authentication(ops_test: OpsTest, charm_versions):
    """Test that charm is stopped when authentication is disabled."""
    logger.info("Removing relation between postgresql-k8s and kyuubi-k8s over auth-db endpoint...")
    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:auth-db", f"{charm_versions.auth_db.application_name}:database"
    )

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], timeout=1000, status="blocked", idle_period=10
    )

    with pytest.raises(TTransportException):
        assert await validate_sql_queries_with_kyuubi(ops_test=ops_test)
