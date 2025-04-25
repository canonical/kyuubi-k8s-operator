#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import psycopg2
import pytest
import yaml
from thrift.transport.TTransport import TTransportException

from constants import (
    AUTHENTICATION_DATABASE_NAME,
    KYUUBI_CLIENT_RELATION_NAME,
)

from .helpers import (
    deploy_minimal_kyuubi_setup,
    validate_sql_queries_with_kyuubi,
)
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
TEST_CHARM_NAME = "application"


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

    # Assert that all charms that were deployed as part of minimal setup are in correct states.
    juju.wait(jubilant.all_active, timeout=120, delay=3)


def test_enable_authentication(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Enable authentication for Kyuubi."""
    logger.info("Deploying postgresql-k8s charm...")
    juju.deploy(**charm_versions.postgres.deploy_dict())

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME, charm_versions.postgres.app),
        delay=5,
    )

    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm...")
    juju.integrate(charm_versions.postgres.app, f"{APP_NAME}:auth-db")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME, charm_versions.postgres.app),
        delay=10,
    )


def test_kyuubi_client_relation_joined(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, test_charm: Path
) -> None:
    """Test behavior of Kyuubi charm when a client application is related to it."""
    # Deploy the test charm and wait for waiting status
    logger.info("Deploying test charm...")
    juju.deploy(test_charm, app=TEST_CHARM_NAME, num_units=1)

    logger.info("Waiting for test charm to be idle...")
    status = juju.wait(lambda status: jubilant.all_active(status, APP_NAME, TEST_CHARM_NAME))

    # Check number of users before integration
    # Fetch password for operator user from postgresql-k8s
    postgres_leader = f"{charm_versions.postgres.app}/0"
    postgres_host = status.apps[charm_versions.postgres.app].units[postgres_leader].address

    task = juju.run(postgres_leader, "get-password")
    assert task.return_code == 0
    password = task.results["password"]

    # Connect to PostgreSQL metastore database
    with (
        psycopg2.connect(
            host=postgres_host,
            database=AUTHENTICATION_DATABASE_NAME,
            user="operator",
            password=password,
        ) as connection,
        connection.cursor() as cursor,
    ):
        # Fetch number of users excluding the default admin user
        cursor.execute(""" SELECT username, passwd FROM kyuubi_users WHERE username <> 'admin' """)
        num_users = cursor.rowcount

    assert num_users == 0

    logger.info("Integrating test charm with kyuubi-k8s charm...")
    juju.integrate(APP_NAME, TEST_CHARM_NAME)

    logger.info("Waiting for test-charm and kyuubi charm to be idle and active...")
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME, TEST_CHARM_NAME), delay=10)

    with (
        psycopg2.connect(
            host=postgres_host,
            database=AUTHENTICATION_DATABASE_NAME,
            user="operator",
            password=password,
        ) as connection,
        connection.cursor() as cursor,
    ):
        # Fetch number of users excluding the default admin user
        cursor.execute(""" SELECT username, passwd FROM kyuubi_users WHERE username <> 'admin' """)
        num_users = cursor.rowcount
        kyuubi_username, kyuubi_password = cursor.fetchone()  # type: ignore

    # A new user has indeed been created
    assert num_users != 0

    logger.info(f"Relation user's username: {kyuubi_username} and password: {kyuubi_password}")

    assert validate_sql_queries_with_kyuubi(
        juju=juju, username=kyuubi_username, password=kyuubi_password
    )


def test_kyuubi_client_relation_removed(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test the behavior of Kyuubi when client application relation is removed from it."""
    status = juju.wait(lambda status: jubilant.all_active(status, APP_NAME, TEST_CHARM_NAME))
    # Fetch password for operator user from postgresql-k8s
    postgres_leader = f"{charm_versions.postgres.app}/0"
    postgres_host = status.apps[charm_versions.postgres.app].units[postgres_leader].address

    task = juju.run(postgres_leader, "get-password")
    assert task.return_code == 0
    password = task.results["password"]

    # Connect to PostgreSQL metastore database
    with (
        psycopg2.connect(
            host=postgres_host,
            database=AUTHENTICATION_DATABASE_NAME,
            user="operator",
            password=password,
        ) as connection,
        connection.cursor() as cursor,
    ):
        # Fetch number of users excluding the default admin user
        cursor.execute(""" SELECT username, passwd FROM kyuubi_users WHERE username <> 'admin' """)
        num_users_before = cursor.rowcount
        kyuubi_username, kyuubi_password = cursor.fetchone()  # type: ignore

    logger.info(f"Relation user's username: {kyuubi_username} and password: {kyuubi_password}")
    assert num_users_before != 0

    assert validate_sql_queries_with_kyuubi(
        juju=juju, username=kyuubi_username, password=kyuubi_password
    )

    logger.info("Removing relation between test charm and kyuubi-k8s...")
    juju.remove_relation(
        f"{APP_NAME}:{KYUUBI_CLIENT_RELATION_NAME}",
        f"{TEST_CHARM_NAME}:{KYUUBI_CLIENT_RELATION_NAME}",
    )

    logger.info("Waiting for test-charm and kyuubi charm to be idle and active...")
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME, TEST_CHARM_NAME), delay=3)

    # Fetch number of users excluding the default admin user
    with (
        psycopg2.connect(
            host=postgres_host,
            database=AUTHENTICATION_DATABASE_NAME,
            user="operator",
            password=password,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(""" SELECT username, passwd FROM kyuubi_users WHERE username <> 'admin' """)
        num_users_after = cursor.rowcount

    # Assert that relation user created previously has been deleted
    assert num_users_after == 0

    with pytest.raises(TTransportException) as exc:
        validate_sql_queries_with_kyuubi(
            juju=juju, username=kyuubi_username, password=kyuubi_password
        )
        assert b"Error validating the login" in getattr(exc.value, "message", b"")
