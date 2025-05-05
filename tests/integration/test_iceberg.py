#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import yaml
from spark_test.core.kyuubi import KyuubiClient

from .helpers import (
    deploy_minimal_kyuubi_setup,
    fetch_password,
    get_leader_unit,
)
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def test_deploy_kyuubi_setup(
    juju: jubilant.Juju,
    kyuubi_charm: Path,
    charm_versions: IntegrationTestsCharms,
    s3_bucket_and_creds: S3Info,
) -> None:
    """Deploy the minimal setup for Kyuubi and assert all charms are in active and idle state."""
    deploy_minimal_kyuubi_setup(
        juju=juju,
        kyuubi_charm=kyuubi_charm,
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=True,
    )

    # Wait for everything to settle down
    juju.wait(jubilant.all_active, delay=5)


def test_iceberg_with_iceberg_catalog(juju: jubilant.Juju) -> None:
    """Test Iceberg capabilities using the `iceberg` catalog created by default."""
    status = juju.status()
    leader = get_leader_unit(juju, APP_NAME)
    host = status.apps[APP_NAME].units[leader].address
    port = 10009
    username = "admin"
    password = fetch_password(juju)

    # Put some load by executing some Kyuubi SQL queries
    kyuubi_client = KyuubiClient(host=host, port=port, username=username, password=password)

    with kyuubi_client.connection as conn, conn.cursor() as cursor:
        cursor.execute("USE iceberg;")
        cursor.execute("CREATE DATABASE idb;")
        cursor.execute("USE idb;")
        cursor.execute("CREATE TABLE itable (id BIGINT) USING iceberg;")
        cursor.execute("INSERT INTO itable VALUES (12345);")
        cursor.execute("SELECT * FROM itable;")
        results = cursor.fetchall()
        assert len(results) == 1


def test_iceberg_external_metastore(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test Iceberg support with Postgres as external metastore."""
    # Deploy the charm and wait for waiting status
    logger.info("Deploying postgresql-k8s charm...")
    juju.deploy(**charm_versions.metastore_db.deploy_dict())

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    juju.wait(jubilant.all_active, delay=15, timeout=1000)

    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm...")
    juju.integrate(charm_versions.metastore_db.app, f"{APP_NAME}:metastore-db")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    status = juju.wait(jubilant.all_active, delay=20, timeout=1000)

    leader = get_leader_unit(juju, APP_NAME)
    host = status.apps[APP_NAME].units[leader].address
    port = 10009
    username = "admin"
    password = fetch_password(juju)

    kyuubi_client = KyuubiClient(host=host, port=port, username=username, password=password)

    with kyuubi_client.connection as conn, conn.cursor() as cursor:
        cursor.execute("USE iceberg;")
        cursor.execute("CREATE DATABASE dbi;")
        cursor.execute("USE dbi;")
        cursor.execute("CREATE TABLE tablei (id BIGINT) USING iceberg;")
        cursor.execute("INSERT INTO tablei VALUES (12345);")
        cursor.execute("SELECT * FROM tablei;")
        results = cursor.fetchall()
        assert len(results) == 1


def test_disconnect_and_reconnect_external_metastore(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test disconnecting external metastore and reconnecting to it again and read old data."""
    logger.info("Removing relation between postgresql-k8s and kyuubi-k8s...")
    juju.remove_relation(f"{APP_NAME}:metastore-db", f"{charm_versions.metastore_db.app}:database")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    juju.wait(jubilant.all_active, delay=10)

    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm again...")
    juju.integrate(charm_versions.metastore_db.application_name, f"{APP_NAME}:metastore-db")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    status = juju.wait(jubilant.all_active, delay=20)

    leader = get_leader_unit(juju, APP_NAME)
    host = status.apps[APP_NAME].units[leader].address
    port = 10009
    username = "admin"
    password = fetch_password(juju)

    kyuubi_client = KyuubiClient(host=host, port=port, username=username, password=password)

    # Verify that the previously inserted rows are readable
    with kyuubi_client.connection as conn, conn.cursor() as cursor:
        cursor.execute("USE iceberg;")
        cursor.execute("USE dbi;")
        cursor.execute("SELECT * FROM tablei;")
        results = cursor.fetchall()
        assert len(results) == 1


# Test normal tables can be written / read using the iceberg catalog
def test_normal_table_format_with_iceberg_catalog(juju: jubilant.Juju) -> None:
    """Test that tables using non-iceberg format can be read and written using iceberg catalog."""
    status = juju.status()
    leader = get_leader_unit(juju, APP_NAME)
    host = status.apps[APP_NAME].units[leader].address
    port = 10009
    username = "admin"
    password = fetch_password(juju)

    kyuubi_client = KyuubiClient(host=host, port=port, username=username, password=password)

    # Verify that the data can be read and written using default (non-iceberg) table format
    with kyuubi_client.connection as conn, conn.cursor() as cursor:
        cursor.execute("USE iceberg;")
        cursor.execute("CREATE DATABASE normal_idb;")
        cursor.execute("USE normal_idb;")
        cursor.execute("CREATE TABLE normal_itable (id INT);")
        cursor.execute("INSERT INTO normal_itable VALUES (12345);")
        cursor.execute("SELECT * FROM normal_itable;")
        results = cursor.fetchall()
        assert len(results) == 1


def test_iceberg_with_spark_catalog(juju: jubilant.Juju) -> None:
    """Test running Kyuubi SQL queries when dynamic allocation option is disabled in Kyuubi charm."""
    logger.info("Changing Iceberg catalog to default spark_catalog...")
    juju.config(APP_NAME, {"iceberg-catalog-name": "spark_catalog"})

    logger.info("Waiting for kyuubi-k8s app to be active and idle...")
    status = juju.wait(jubilant.all_active, delay=5)
    leader = get_leader_unit(juju, APP_NAME)
    host = status.apps[APP_NAME].units[leader].address
    port = 10009
    username = "admin"
    password = fetch_password(juju)

    # Put some load by executing some Kyuubi SQL queries
    kyuubi_client = KyuubiClient(host=host, port=port, username=username, password=password)

    with kyuubi_client.connection as conn, conn.cursor() as cursor:
        cursor.execute("USE spark_catalog;")
        cursor.execute("CREATE DATABASE sdb;")
        cursor.execute("USE sdb;")
        cursor.execute("CREATE TABLE stable (id BIGINT) USING iceberg;")
        cursor.execute("INSERT INTO stable VALUES (12345);")
        cursor.execute("SELECT * FROM stable;")
        results = cursor.fetchall()
        assert len(results) == 1


def test_reading_table_written_by_other_catalog(juju: jubilant.Juju) -> None:
    """Test whether one is able to read data written using iceberg catalog using spark_catalog."""
    status = juju.status()
    leader = get_leader_unit(juju, APP_NAME)
    host = status.apps[APP_NAME].units[leader].address
    port = 10009
    username = "admin"
    password = fetch_password(juju)

    kyuubi_client = KyuubiClient(host=host, port=port, username=username, password=password)

    # Verify that the previously inserted rows are readable
    with kyuubi_client.connection as conn, conn.cursor() as cursor:
        cursor.execute("USE spark_catalog;")
        cursor.execute("USE dbi;")
        cursor.execute("SELECT * FROM tablei;")
        results = cursor.fetchall()
        assert len(results) == 1


def test_normal_table_format_with_iceberg_enabled_spark_catalog(juju: jubilant.Juju) -> None:
    """Test that tables using non-iceberg format can be read and written using iceberg enabled spark_catalog."""
    status = juju.status()
    leader = get_leader_unit(juju, APP_NAME)
    host = status.apps[APP_NAME].units[leader].address
    port = 10009
    username = "admin"
    password = fetch_password(juju)

    kyuubi_client = KyuubiClient(host=host, port=port, username=username, password=password)

    # Verify that the data can be read and written using default (non-iceberg) table format
    with kyuubi_client.connection as conn, conn.cursor() as cursor:
        cursor.execute("USE spark_catalog;")
        cursor.execute("CREATE DATABASE normal_sdb;")
        cursor.execute("USE normal_sdb;")
        cursor.execute("CREATE TABLE normal_stable (id INT);")
        cursor.execute("INSERT INTO normal_stable VALUES (12345);")
        cursor.execute("SELECT * FROM normal_stable;")
        results = cursor.fetchall()
        assert len(results) == 1
