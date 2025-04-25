#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import logging
import uuid
from pathlib import Path

import jubilant
import psycopg2
import pytest
import yaml
from thrift.transport.TTransport import TTransportException

from constants import AUTHENTICATION_DATABASE_NAME

from .helpers import (
    deploy_minimal_kyuubi_setup,
    validate_sql_queries_with_kyuubi,
)
from .types import IntegrationTestsCharms

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def test_deploy_minimal_kyuubi_setup(
    juju: jubilant.Juju,
    kyuubi_charm: Path,
    charm_versions,
    s3_bucket_and_creds,
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
    juju.wait(jubilant.all_active, delay=5)


def test_sql_queries_no_authentication(juju: jubilant.Juju) -> None:
    """Test running SQL queries when authentication has not yet been enabled."""
    assert validate_sql_queries_with_kyuubi(juju=juju)


def test_enable_authentication(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test the Kyuuubi charm by integrating it with external metastore."""
    # Deploy the charm and wait for waiting status
    logger.info("Deploying postgresql-k8s charm...")
    juju.deploy(**charm_versions.postgres.deploy_dict())

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    juju.wait(
        lambda status: jubilant.all_active(
            status, APP_NAME, charm_versions.postgres.application_name
        ),
        timeout=600,
        delay=3,
    )

    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm...")
    juju.integrate(charm_versions.postgres.application_name, f"{APP_NAME}:auth-db")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    status = juju.wait(
        lambda status: jubilant.all_active(
            status, APP_NAME, charm_versions.postgres.application_name
        ),
        timeout=300,
        delay=10,
    )

    postgres_leader = f"{charm_versions.postgres.application_name}/0"
    postgres_host = (
        status.apps[charm_versions.postgres.application_name].units[postgres_leader].address
    )

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
        cursor.execute(""" SELECT * FROM kyuubi_users; """)
        assert cursor.rowcount != 0


def test_kyuubi_without_passing_credentials(juju: jubilant.Juju) -> None:
    """Test the JDBC connection when invalid credentials are provided."""
    with pytest.raises(TTransportException) as exc:
        validate_sql_queries_with_kyuubi(juju=juju)
    assert b"Error validating the login" in getattr(exc.value, "message", b"")


def test_kyuubi_with_invalid_credentials(juju: jubilant.Juju) -> None:
    """Test the JDBC connection when invalid credentials are provided."""
    username = "admin"
    password = str(uuid.uuid4())
    with pytest.raises(TTransportException) as exc:
        validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)
    assert b"Error validating the login" in getattr(exc.value, "message", b"")


def test_kyuubi_valid_credentials(juju: jubilant.Juju) -> None:
    """Test the JDBC connection when invalid credentials are provided."""
    logger.info("Running action 'get-password' on kyuubi unit")
    task = juju.run(f"{APP_NAME}/0", "get-password")
    assert task.return_code == 0
    password = task.results["password"]

    logger.info(f"Fetched password: {password}")

    username = "admin"
    assert validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)


def test_set_password_action(juju: jubilant.Juju) -> None:
    """Test set-password action."""
    logger.info("Running action 'set-password' on kyuubi-k8s unit...")
    new_password = str(uuid.uuid4())

    task = juju.run(f"{APP_NAME}/0", "set-password", {"password": new_password})
    assert task.return_code == 0

    logger.info("Running action 'get-password' on kyuubi unit")
    task = juju.run(f"{APP_NAME}/0", "get-password")
    assert task.return_code == 0
    assert new_password == task.results["password"]

    username = "admin"
    assert validate_sql_queries_with_kyuubi(juju=juju, username=username, password=new_password)


def test_remove_authentication(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test the JDBC connection when authentication is disabled."""
    logger.info("Removing relation between postgresql-k8s and kyuubi-k8s over auth-db endpoint...")
    juju.remove_relation(
        f"{APP_NAME}:auth-db", f"{charm_versions.postgres.application_name}:database"
    )

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    juju.wait(
        lambda status: jubilant.all_active(
            status, APP_NAME, charm_versions.postgres.application_name
        ),
        timeout=120,
        delay=3,
    )

    assert validate_sql_queries_with_kyuubi(juju=juju)
