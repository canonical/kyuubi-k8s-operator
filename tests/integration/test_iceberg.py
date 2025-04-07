#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import pytest
import yaml
from spark_test.core.kyuubi import KyuubiClient

from core.domain import Status

from .helpers import (
    check_status,
    deploy_minimal_kyuubi_setup,
    get_address,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
TEST_CHARM_PATH = "./tests/integration/app-charm"
TEST_CHARM_NAME = "application"


@pytest.mark.abort_on_fail
async def test_deploy_kyuubi_setup(
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
async def test_iceberg_with_iceberg_catalog(ops_test):
    """Test running Kyuubi SQL queries when dynamic allocation option is disabled in Kyuubi charm."""
    host = await get_address(ops_test, unit_name=f"{APP_NAME}/0")
    port = 10009

    # Put some load by executing some Kyuubi SQL queries
    kyuubi_client = KyuubiClient(host=host, port=int(port))

    with kyuubi_client.connection as conn, conn.cursor() as cursor:
        cursor.execute("USE iceberg;")
        cursor.execute("CREATE DATABASE idb;")
        cursor.execute("CREATE TABLE itable (id BIGINT) USING iceberg;")
        cursor.execute("INSERT INTO itable VALUES (12345);")
        cursor.execute("SELECT * FROM itable;")
        results = cursor.fetchall()
        assert len(results) == 1


@pytest.mark.abort_on_fail
async def test_iceberg_external_metastore(ops_test, charm_versions):
    """Test Iceberg support with Postgres as external metastore."""
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

    host = await get_address(ops_test, unit_name=f"{APP_NAME}/0")
    port = 10009

    kyuubi_client = KyuubiClient(host=host, port=int(port))

    with kyuubi_client.connection as conn, conn.cursor() as cursor:
        cursor.execute("USE spark_catalog;")
        cursor.execute("CREATE DATABASE sdb;")
        cursor.execute("CREATE TABLE stable (id BIGINT) USING iceberg;")
        cursor.execute("INSERT INTO stable VALUES (12345);")
        cursor.execute("SELECT * FROM stable;")
        results = cursor.fetchall()
        assert len(results) == 1


@pytest.mark.abort_on_fail
async def test_iceberg_with_spark_catalog(ops_test):
    """Test running Kyuubi SQL queries when dynamic allocation option is disabled in Kyuubi charm."""
    logger.info("Changing Iceberg catalog to default spark_catalog...")
    await ops_test.model.applications[APP_NAME].set_config(
        {"iceberg-catalog-name": "spark_catalog"}
    )
    logger.info("Waiting for kyuubi-k8s app to be active and idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )

    host = await get_address(ops_test, unit_name=f"{APP_NAME}/0")
    port = 10009

    # Put some load by executing some Kyuubi SQL queries
    kyuubi_client = KyuubiClient(host=host, port=int(port))

    with kyuubi_client.connection as conn, conn.cursor() as cursor:
        cursor.execute("USE spark_catalog;")
        cursor.execute("CREATE DATABASE sdb;")
        cursor.execute("CREATE TABLE stable (id BIGINT) USING iceberg;")
        cursor.execute("INSERT INTO stable VALUES (12345);")
        cursor.execute("SELECT * FROM stable;")
        results = cursor.fetchall()
        assert len(results) == 1
