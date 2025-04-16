#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import logging
import time
from pathlib import Path

import psycopg2
import pytest
import yaml

from constants import METASTORE_DATABASE_NAME
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
TEST_EXTERNAL_DB_NAME = "dbext"
TEST_EXTERNAL_TABLE_NAME = "text"


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
async def test_sql_queries_local_metastore(ops_test):
    """Test running SQL queries without an external metastore."""
    assert await validate_sql_queries_with_kyuubi(ops_test=ops_test)


@pytest.mark.abort_on_fail
async def test_integrate_external_metastore(ops_test, charm_versions):
    """Test the Kyuuubi charm by integrating it with external metastore."""
    # Deploy the charm and wait for waiting status
    logger.info("Deploying postgresql-k8s charm...")
    await ops_test.model.deploy(**charm_versions.postgres.deploy_dict())

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.postgres.application_name], timeout=1000, status="active"
    )

    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm...")
    await ops_test.model.integrate(
        charm_versions.postgres.application_name, f"{APP_NAME}:metastore-db"
    )

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
        database=METASTORE_DATABASE_NAME,
        user="operator",
        password=password,
    )

    with connection.cursor() as cursor:
        cursor.execute(""" SELECT * FROM "VERSION"; """)
        assert cursor.rowcount == 1

    connection.close()


@pytest.mark.abort_on_fail
async def test_sql_queries_external_metastore(ops_test):
    """Test running SQL queries with an external metastore."""
    assert await validate_sql_queries_with_kyuubi(
        ops_test=ops_test, db_name=TEST_EXTERNAL_DB_NAME, table_name=TEST_EXTERNAL_TABLE_NAME
    )


@pytest.mark.abort_on_fail
async def test_remove_external_metastore(ops_test, charm_versions):
    """Test removal of external metastore."""
    logger.info("Removing relation between postgresql-k8s and kyuubi-k8s...")
    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:metastore-db", f"{charm_versions.postgres.application_name}:database"
    )

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.postgres.application_name], timeout=1000, status="active"
    )

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
        database=METASTORE_DATABASE_NAME,
        user="operator",
        password=password,
    )

    with connection.cursor() as cursor:
        cursor.execute(""" SELECT * FROM "VERSION"; """)
        assert cursor.rowcount == 1

    connection.close()


@pytest.mark.abort_on_fail
async def test_run_sql_queries_again_with_local_metastore(ops_test, charm_versions):
    """Test running SQL queries again with local metastore."""
    logger.info(
        "Waiting for extra 30 seconds as cool-down period before proceeding with the test..."
    )
    time.sleep(30)

    assert await validate_sql_queries_with_kyuubi(ops_test=ops_test)


async def test_prepare_metastore_with_invalid_schema(ops_test, charm_versions):
    """Prepare an external metastore with invalid schema."""
    # Deploy the charm and wait for waiting status
    logger.info(
        f"Deploying a new instance of postgresql-k8s charm with alias {INVALID_METASTORE_APP_NAME}..."
    )
    deploy_dict = charm_versions.postgres.deploy_dict()
    deploy_dict.update({"application_name": INVALID_METASTORE_APP_NAME})
    await ops_test.model.deploy(**deploy_dict)

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, INVALID_METASTORE_APP_NAME], timeout=1000, status="active"
    )

    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm...")
    await ops_test.model.integrate(INVALID_METASTORE_APP_NAME, f"{APP_NAME}:metastore-db")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    await ops_test.model.wait_for_idle(apps=[APP_NAME, INVALID_METASTORE_APP_NAME], timeout=1000)

    # Assert that both kyuubi-k8s and postgresql-k8s charms are in active state
    assert check_status(ops_test.model.applications[APP_NAME], Status.ACTIVE.value)
    assert ops_test.model.applications[INVALID_METASTORE_APP_NAME].status == "active"

    # By this time, the postgres database will have been initialized with the Hive metastore schema
    # Now remove the relation to mutate the schema externally

    logger.info("Removing relation between postgresql-k8s and kyuubi-k8s...")
    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:metastore-db", f"{INVALID_METASTORE_APP_NAME}:database"
    )

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, INVALID_METASTORE_APP_NAME], timeout=1000, status="active"
    )

    # Now attempt to mutate the schema
    logger.info("Mutating metastore schema to make it invalid...")
    postgres_leader = await find_leader_unit(ops_test, app_name=INVALID_METASTORE_APP_NAME)
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
        database=METASTORE_DATABASE_NAME,
        user="operator",
        password=password,
    )

    with connection.cursor() as cursor:
        cursor.execute(""" DROP TABLE "VERSION"; """)
        cursor.execute(""" DROP TABLE "DBS" CASCADE; """)
        connection.commit()

    connection.close()


@pytest.mark.abort_on_fail
async def test_integrate_metastore_with_invalid_schema(ops_test):
    """Test the charm by integrating it with metastore with invalid schema."""
    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s (invalid-metastore) charm...")
    await ops_test.model.integrate(INVALID_METASTORE_APP_NAME, f"{APP_NAME}:metastore-db")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    await ops_test.model.wait_for_idle(apps=[APP_NAME, INVALID_METASTORE_APP_NAME], timeout=1000)

    # Assert that postgresql-k8s charm is in active state
    assert ops_test.model.applications[INVALID_METASTORE_APP_NAME].status == "active"

    # Assert that Kyuubi charm is in blocked state, complaining about invalid schema
    assert check_status(
        ops_test.model.applications[APP_NAME], Status.INVALID_METASTORE_SCHEMA.value
    )


@pytest.mark.abort_on_fail
async def test_integrate_metastore_with_valid_schema_again(ops_test, charm_versions):
    """Test integration of Kyuubi charm with external metastore with valid schema again."""
    logger.info("Removing relation between postgresql-k8s and kyuubi-k8s...")
    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:metastore-db", f"{INVALID_METASTORE_APP_NAME}:database"
    )

    logger.info("Waiting for kyuubi-k8s app to be idle and active...")
    await ops_test.model.wait_for_idle(apps=[APP_NAME], timeout=1000, status="active")
    # Assert that Kyuubi charm is in active state again
    assert check_status(ops_test.model.applications[APP_NAME], Status.ACTIVE.value)

    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm again...")
    await ops_test.model.integrate(
        charm_versions.postgres.application_name, f"{APP_NAME}:metastore-db"
    )

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.postgres.application_name], timeout=1000
    )

    # Assert that postgresql-k8s charm is in active state
    assert ops_test.model.applications[INVALID_METASTORE_APP_NAME].status == "active"

    # Assert that Kyuubi charm is in active state again
    assert check_status(ops_test.model.applications[APP_NAME], Status.ACTIVE.value)


@pytest.mark.abort_on_fail
async def test_read_write_with_valid_schema_metastore_again(ops_test):
    """Test whether previously written data can be read as well as new data can be written."""
    assert await validate_sql_queries_with_kyuubi(
        ops_test=ops_test,
        query_lines=[
            f"USE {TEST_EXTERNAL_DB_NAME};",
            f"SELECT * FROM {TEST_EXTERNAL_TABLE_NAME};",
        ],
    )
    assert await validate_sql_queries_with_kyuubi(ops_test=ops_test)
