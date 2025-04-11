#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import logging
import time
import uuid
from pathlib import Path

import psycopg2
import pytest
import yaml
from pytest_operator.plugin import OpsTest
from thrift.transport.TTransport import TTransportException

from constants import AUTHENTICATION_DATABASE_NAME
from core.domain import Status

from .helpers import (
    check_status,
    deploy_minimal_kyuubi_setup,
    find_leader_unit,
    get_address,
    validate_sql_queries_with_kyuubi,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
TEST_CHARM_PATH = "./tests/integration/app-charm"
TEST_CHARM_NAME = "application"
INVALID_METASTORE_APP_NAME = "invalid-metastore"


@pytest.mark.abort_on_fail
async def test_deploy_minimal_kyuubi_setup(
    ops_test,
    kyuubi_charm,
    charm_versions,
    s3_bucket_and_creds,
):
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
        ],
        idle_period=20,
        status="active",
    )

    # Assert that all charms that were deployed as part of minimal setup are in correct states.
    assert check_status(ops_test.model.applications[APP_NAME], Status.ACTIVE.value)
    assert (
        ops_test.model.applications[charm_versions.integration_hub.application_name].status
        == "active"
    )
    assert ops_test.model.applications[charm_versions.s3.application_name].status == "active"


@pytest.mark.abort_on_fail
async def test_sql_queries_no_authentication(ops_test):
    """Test running SQL queries when authentication has not yet been enabled."""
    assert await validate_sql_queries_with_kyuubi(ops_test=ops_test)


@pytest.mark.abort_on_fail
async def test_enable_authentication(ops_test, charm_versions):
    """Test the Kyuuubi charm by integrating it with external metastore."""
    # Deploy the charm and wait for waiting status
    logger.info("Deploying postgresql-k8s charm...")
    await ops_test.model.deploy(**charm_versions.postgres.deploy_dict())

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.postgres.application_name], timeout=1000, status="active"
    )

    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm...")
    await ops_test.model.integrate(charm_versions.postgres.application_name, f"{APP_NAME}:auth-db")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.postgres.application_name], timeout=1000
    )

    # Assert that both kyuubi-k8s and postgresql-k8s charms are in active state
    assert check_status(ops_test.model.applications[APP_NAME], Status.ACTIVE.value)
    assert ops_test.model.applications[charm_versions.postgres.application_name].status == "active"

    postgres_leader = await find_leader_unit(
        ops_test, app_name=charm_versions.postgres.application_name
    )
    assert postgres_leader is not None
    postgres_host = await get_address(ops_test, unit_name=postgres_leader.name)

    action = await postgres_leader.run_action(
        action_name="get-password",
    )
    result = await action.wait()
    password = result.results.get("password")

    # Connect to PostgreSQL metastore database
    connection = psycopg2.connect(
        host=postgres_host,
        database=AUTHENTICATION_DATABASE_NAME,
        user="operator",
        password=password,
    )

    with connection.cursor() as cursor:
        cursor.execute(""" SELECT * FROM kyuubi_users; """)
        assert cursor.rowcount != 0

    connection.close()


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
async def test_remove_authentication(ops_test: OpsTest, test_pod, charm_versions):
    """Test the JDBC connection when authentication is disabled."""
    logger.info("Removing relation between postgresql-k8s and kyuubi-k8s over auth-db endpoint...")
    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:auth-db", f"{charm_versions.postgres.application_name}:database"
    )

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.postgres.application_name], timeout=1000, status="active"
    )

    logger.info(
        "Waiting for extra 30 seconds as cool-down period before proceeding with the test..."
    )
    time.sleep(30)

    assert await validate_sql_queries_with_kyuubi(ops_test=ops_test)
