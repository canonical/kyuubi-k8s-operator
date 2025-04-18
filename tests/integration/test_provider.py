#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import logging
import time
from pathlib import Path

import psycopg2
import pytest
import yaml
from pytest_operator.plugin import OpsTest
from thrift.transport.TTransport import TTransportException

from constants import (
    AUTHENTICATION_DATABASE_NAME,
    KYUUBI_CLIENT_RELATION_NAME,
)
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
COS_AGENT_APP_NAME = "grafana-agent-k8s"


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
async def test_enable_authentication(ops_test: OpsTest, charm_versions):
    """Enable authentication for Kyuubi."""
    logger.info("Deploying postgresql-k8s charm...")
    await ops_test.model.deploy(**charm_versions.postgres.deploy_dict())

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.postgres.application_name], timeout=1000, status="active"
    )

    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm over auth-db endpoint...")
    await ops_test.model.integrate(charm_versions.postgres.application_name, f"{APP_NAME}:auth-db")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.postgres.application_name], timeout=1000
    )

    # Assert that both kyuubi-k8s and postgresql-k8s charms are in active state
    assert check_status(ops_test.model.applications[APP_NAME], Status.ACTIVE.value)
    assert ops_test.model.applications[charm_versions.postgres.application_name].status == "active"


@pytest.mark.abort_on_fail
async def test_kyuubi_client_relation_joined(ops_test: OpsTest, charm_versions):
    """Test behavior of Kyuubi charm when a client application is related to it."""
    logger.info("Building test charm (app-charm)...")
    app_charm = await ops_test.build_charm(TEST_CHARM_PATH)

    # Deploy the test charm and wait for waiting status
    logger.info("Deploying test charm...")
    await ops_test.model.deploy(
        app_charm,
        application_name=TEST_CHARM_NAME,
        num_units=1,
        series="jammy",
    )

    logger.info("Waiting for test charm to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[TEST_CHARM_NAME, APP_NAME], timeout=1000, status="active"
    )

    # Check number of users before integration
    # Fetch password for operator user from postgresql-k8s
    postgres_unit = ops_test.model.applications[charm_versions.postgres.application_name].units[0]
    action = await postgres_unit.run_action(
        action_name="get-password",
    )
    result = await action.wait()
    password = result.results.get("password")

    # Fetch host address of postgresql-k8s
    status = await ops_test.model.get_status()
    postgresql_host_address = status["applications"][charm_versions.postgres.application_name][
        "units"
    ][f"{charm_versions.postgres.application_name}/0"]["address"]

    # Connect to PostgreSQL authentication database
    connection = psycopg2.connect(
        host=postgresql_host_address,
        database=AUTHENTICATION_DATABASE_NAME,
        user="operator",
        password=password,
    )

    # Fetch number of users excluding the default admin user
    with connection.cursor() as cursor:
        cursor.execute(""" SELECT username, passwd FROM kyuubi_users WHERE username <> 'admin' """)
        num_users = cursor.rowcount

    assert num_users == 0

    logger.info("Integrating test charm with kyuubi-k8s charm...")
    await ops_test.model.integrate(TEST_CHARM_NAME, APP_NAME)

    logger.info("Waiting for test-charm and kyuubi charm to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, TEST_CHARM_NAME], timeout=1000, status="active"
    )

    logger.info(
        "Waiting for extra 30 seconds as cool-down period before proceeding with the test..."
    )
    time.sleep(30)

    # Fetch number of users excluding the default admin user
    with connection.cursor() as cursor:
        cursor.execute(""" SELECT username, passwd FROM kyuubi_users WHERE username <> 'admin' """)
        num_users = cursor.rowcount
        kyuubi_username, kyuubi_password = cursor.fetchone()

    connection.close()

    # Assert that a new user had indeed been created
    assert num_users != 0

    logger.info(f"Relation user's username: {kyuubi_username} and password: {kyuubi_password}")

    assert await validate_sql_queries_with_kyuubi(
        ops_test=ops_test, username=kyuubi_username, password=kyuubi_password
    )


@pytest.mark.abort_on_fail
async def test_kyuubi_client_relation_removed(ops_test: OpsTest, charm_versions):
    """Test the behavior of Kyuubi when client application relation is removed from it."""
    logger.info("Waiting for charms to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[TEST_CHARM_NAME, APP_NAME], timeout=1000, status="active"
    )

    # Fetch host address of postgresql-k8s
    postgres_unit = await find_leader_unit(
        ops_test, app_name=charm_versions.postgres.application_name
    )
    assert postgres_unit is not None
    postgresql_host_address = await get_address(ops_test, unit_name=postgres_unit.name)

    # Fetch password for operator user from postgresql-k8s
    action = await postgres_unit.run_action(
        action_name="get-password",
    )
    result = await action.wait()
    password = result.results.get("password")

    # Connect to PostgreSQL metastore database
    connection = psycopg2.connect(
        host=postgresql_host_address,
        database=AUTHENTICATION_DATABASE_NAME,
        user="operator",
        password=password,
    )

    # Fetch number of users excluding the default admin user
    with connection.cursor() as cursor:
        cursor.execute(
            """ SELECT username, passwd FROM kyuubi_users WHERE username <> 'admin'; """
        )
        num_users_before = cursor.rowcount
        kyuubi_username, kyuubi_password = cursor.fetchone()

    logger.info(f"Relation user's username: {kyuubi_username} and password: {kyuubi_password}")
    assert num_users_before != 0

    assert await validate_sql_queries_with_kyuubi(
        ops_test=ops_test, username=kyuubi_username, password=kyuubi_password
    )

    logger.info("Removing relation between test charm and kyuubi-k8s...")
    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:{KYUUBI_CLIENT_RELATION_NAME}",
        f"{TEST_CHARM_NAME}:{KYUUBI_CLIENT_RELATION_NAME}",
    )

    logger.info("Waiting for test-charm and kyuubi charm to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, TEST_CHARM_NAME], timeout=1000, status="active"
    )

    logger.info(
        "Waiting for extra 30 seconds as cool-down period before proceeding with the test..."
    )
    time.sleep(30)

    # Fetch number of users excluding the default admin user
    with connection.cursor() as cursor:
        cursor.execute(""" SELECT username, passwd FROM kyuubi_users WHERE username <> 'admin' """)
        num_users_after = cursor.rowcount

    connection.close()

    # Assert that relation user created previously has been deleted
    assert num_users_after == 0

    with pytest.raises(TTransportException) as exc:
        await validate_sql_queries_with_kyuubi(
            ops_test=ops_test, username=kyuubi_username, password=kyuubi_password
        )
        assert b"Error validating the login" in exc.value.message
