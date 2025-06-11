#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import logging
import time
from pathlib import Path

import jubilant
import psycopg2
import yaml

from constants import METASTORE_DATABASE_NAME
from core.domain import Status

from .helpers import (
    deploy_minimal_kyuubi_setup,
    fetch_connection_info,
    validate_sql_queries_with_kyuubi,
)
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME: str = METADATA["name"]
INVALID_METASTORE_APP_NAME = "invalid-metastore"
ALT_METASTORE_APP_NAME = "alt-metastore"
TEST_EXTERNAL_DB_NAME = "dbext"
TEST_EXTERNAL_TABLE_NAME = "text"


def test_deploy_minimal_kyuubi_setup(
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
    juju.wait(jubilant.all_active, delay=15)


def test_sql_queries_local_metastore(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test running SQL queries without an external metastore."""
    _, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    assert validate_sql_queries_with_kyuubi(
        juju=juju,
        username=username,
        password=password,
    )


def test_integrate_external_metastore(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test the Kyuuubi charm by integrating it with external metastore."""
    # Deploy the charm and wait for waiting status
    logger.info("Deploying postgresql-k8s charm...")
    juju.deploy(**charm_versions.metastore_db.deploy_dict())

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME, charm_versions.metastore_db.app),
        delay=10,
    )

    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm...")
    juju.integrate(charm_versions.metastore_db.app, f"{APP_NAME}:metastore-db")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    status = juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME, charm_versions.metastore_db.app),
        delay=20,
    )

    postgres_leader = f"{charm_versions.metastore_db.app}/0"
    postgres_host = status.apps[charm_versions.metastore_db.app].units[postgres_leader].address
    task = juju.run(postgres_leader, "get-password")
    assert task.return_code == 0
    password = task.results["password"]

    # Connect to PostgreSQL metastore database
    with (
        psycopg2.connect(
            host=postgres_host,
            database=METASTORE_DATABASE_NAME,
            user="operator",
            password=password,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(""" SELECT * FROM "VERSION"; """)
        assert cursor.rowcount == 1


def test_sql_queries_external_metastore(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test running SQL queries with an external metastore."""
    _, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    assert validate_sql_queries_with_kyuubi(
        juju=juju,
        db_name=TEST_EXTERNAL_DB_NAME,
        table_name=TEST_EXTERNAL_TABLE_NAME,
        username=username,
        password=password,
    )


def test_remove_external_metastore(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test removal of external metastore."""
    logger.info("Removing relation between postgresql-k8s and kyuubi-k8s...")
    juju.remove_relation(f"{APP_NAME}:metastore-db", f"{charm_versions.metastore_db.app}:database")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    status = juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME, charm_versions.metastore_db.app),
        delay=10,
    )

    postgres_leader = f"{charm_versions.metastore_db.app}/0"
    postgres_host = status.apps[charm_versions.metastore_db.app].units[postgres_leader].address
    task = juju.run(postgres_leader, "get-password")
    assert task.return_code == 0
    password = task.results["password"]

    # Connect to PostgreSQL metastore database
    with (
        psycopg2.connect(
            host=postgres_host,
            database=METASTORE_DATABASE_NAME,
            user="operator",
            password=password,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(""" SELECT * FROM "VERSION"; """)
        assert cursor.rowcount == 1


def test_run_sql_queries_again_with_local_metastore(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test running SQL queries again with local metastore."""
    logger.info(
        "Waiting for extra 30 seconds as cool-down period before proceeding with the test..."
    )
    time.sleep(30)
    _, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    assert validate_sql_queries_with_kyuubi(
        juju=juju,
        username=username,
        password=password,
    )


def test_prepare_metastore_with_invalid_schema(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Prepare an external metastore with invalid schema."""
    # Deploy the charm and wait for waiting status
    logger.info(
        f"Deploying a new instance of postgresql-k8s charm with alias {INVALID_METASTORE_APP_NAME}..."
    )
    deploy_dict = charm_versions.metastore_db.deploy_dict()
    deploy_dict.update({"app": INVALID_METASTORE_APP_NAME})
    juju.deploy(**deploy_dict)

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME, INVALID_METASTORE_APP_NAME), delay=10
    )

    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm...")
    juju.integrate(INVALID_METASTORE_APP_NAME, f"{APP_NAME}:metastore-db")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME, INVALID_METASTORE_APP_NAME), delay=20
    )

    # By this time, the postgres database will have been initialized with the Hive metastore schema
    # Now remove the relation to mutate the schema externally

    logger.info("Removing relation between postgresql-k8s and kyuubi-k8s...")
    juju.remove_relation(f"{APP_NAME}:metastore-db", f"{INVALID_METASTORE_APP_NAME}:database")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    status = juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME, INVALID_METASTORE_APP_NAME), delay=10
    )

    # Now attempt to mutate the schema
    logger.info("Mutating metastore schema to make it invalid...")

    postgres_leader = f"{INVALID_METASTORE_APP_NAME}/0"
    postgres_host = status.apps[INVALID_METASTORE_APP_NAME].units[postgres_leader].address

    task = juju.run(postgres_leader, "get-password")
    assert task.return_code == 0
    password = task.results["password"]

    # Connect to PostgreSQL metastore database
    with (
        psycopg2.connect(
            host=postgres_host,
            database=METASTORE_DATABASE_NAME,
            user="operator",
            password=password,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(""" DROP TABLE "VERSION"; """)
        cursor.execute(""" DROP TABLE "DBS" CASCADE; """)
        connection.commit()


def test_integrate_metastore_with_invalid_schema(juju: jubilant.Juju) -> None:
    """Test the charm by integrating it with metastore with invalid schema."""
    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s (invalid-metastore) charm...")
    juju.integrate(INVALID_METASTORE_APP_NAME, f"{APP_NAME}:metastore-db")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    status = juju.wait(lambda status: jubilant.all_blocked(status, APP_NAME))
    assert (
        status.apps[APP_NAME].app_status.message == Status.INVALID_METASTORE_SCHEMA.value.message
    )


def test_integrate_metastore_with_valid_schema_again(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test integration of Kyuubi charm with external metastore with valid schema again."""
    logger.info("Removing relation between postgresql-k8s and kyuubi-k8s...")
    juju.remove_relation(f"{APP_NAME}:metastore-db", f"{INVALID_METASTORE_APP_NAME}:database")

    logger.info("Waiting for kyuubi-k8s app to be idle and active...")
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME))

    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm again...")
    juju.integrate(charm_versions.metastore_db.application_name, f"{APP_NAME}:metastore-db")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    status = juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME, charm_versions.metastore_db.app),
        delay=10,
    )

    # Assert that postgresql-k8s charm is in active state
    assert status.apps[INVALID_METASTORE_APP_NAME].app_status.current == "active"


def test_read_write_with_valid_schema_metastore_again(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test whether previously written data can be read as well as new data can be written."""
    _, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    assert validate_sql_queries_with_kyuubi(
        juju=juju,
        username=username,
        password=password,
        query_lines=[
            f"USE {TEST_EXTERNAL_DB_NAME};",
            f"SELECT * FROM {TEST_EXTERNAL_TABLE_NAME};",
        ],
    )

    assert validate_sql_queries_with_kyuubi(
        juju=juju,
        username=username,
        password=password,
    )


def test_remove_relations(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    """Remove relation between kyuubi and other apps in the model."""
    juju.remove_relation(f"{APP_NAME}:metastore-db", charm_versions.metastore_db.app)
    juju.remove_relation(f"{APP_NAME}:auth-db", charm_versions.auth_db.app)
    juju.remove_relation(APP_NAME, charm_versions.integration_hub.app)
    juju.wait(jubilant.all_agents_idle)
    juju.wait(lambda status: jubilant.all_blocked(status, APP_NAME), delay=5)


def test_metastore_initialization_with_blocked_kyuubi(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test that metastore initialization happens even when Kyuubi is in blocked state."""
    deploy_dict = charm_versions.metastore_db.deploy_dict()
    deploy_dict.update({"app": ALT_METASTORE_APP_NAME})
    logger.info("Deploying new postgresql-k8s charm for metastore...")
    juju.deploy(**deploy_dict)
    juju.wait(lambda status: jubilant.all_active(status, ALT_METASTORE_APP_NAME), delay=5)
    juju.wait(lambda status: jubilant.all_blocked(status, APP_NAME))

    logger.info("Integrate Kyuubi with metastore DB")
    juju.integrate(f"{APP_NAME}:metastore-db", ALT_METASTORE_APP_NAME)
    juju.wait(jubilant.all_agents_idle)
    juju.wait(lambda status: jubilant.all_blocked(status, APP_NAME), delay=5)
    status = juju.wait(
        lambda status: jubilant.all_active(status, ALT_METASTORE_APP_NAME),
    )

    postgres_leader = f"{ALT_METASTORE_APP_NAME}/0"
    postgres_host = status.apps[ALT_METASTORE_APP_NAME].units[postgres_leader].address
    task = juju.run(postgres_leader, "get-password")
    assert task.return_code == 0
    password = task.results["password"]

    # Connect to PostgreSQL metastore database
    with (
        psycopg2.connect(
            host=postgres_host,
            database=METASTORE_DATABASE_NAME,
            user="operator",
            password=password,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(""" SELECT * FROM "VERSION"; """)
        assert cursor.rowcount == 1

    # ssh schematool validate should return 0
    output = juju.ssh(
        f"{APP_NAME}/0",
        command="/opt/hive/bin/schematool.sh -validate -dbType postgres",
        container="kyuubi",
    )
    assert "Done with metastore validation: [SUCCESS]" in output
