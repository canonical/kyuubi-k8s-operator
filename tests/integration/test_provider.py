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
    DEFAULT_ADMIN_USERNAME,
    KYUUBI_CLIENT_RELATION_NAME,
)

from .helpers import (
    deploy_minimal_kyuubi_setup,
    fetch_connection_info,
    get_leader_unit,
    validate_sql_queries_with_kyuubi,
)
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


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
        integrate_data_integrator=False,
    )

    # Assert that all charms that were deployed as part of minimal setup are in correct states.
    juju.wait(jubilant.all_active, delay=15)


def test_deploy_consumer_charm_data_integrator(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
):
    """Deploy consumer charm (data-integrator)."""
    juju.deploy(**charm_versions.data_integrator.deploy_dict(), config={"database-name": "test"})
    logger.info("Waiting for data-integrator charm to be idle...")
    juju.wait(lambda status: jubilant.all_blocked(status, charm_versions.data_integrator.app))


def test_kyuubi_users_before_client_relation(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
):
    # Check number of users before integration
    # Fetch password for operator user from postgresql-k8s
    postgres_leader = get_leader_unit(juju, charm_versions.auth_db.app)
    status = juju.status()
    postgres_host = status.apps[charm_versions.auth_db.app].units[postgres_leader].address

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
        cursor.execute(
            f""" SELECT username, passwd FROM kyuubi_users WHERE username <> '{DEFAULT_ADMIN_USERNAME}' """
        )
        num_users = cursor.rowcount

    assert num_users == 0


def test_kyuubi_client_relation_joined(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, context
) -> None:
    """Test behavior of Kyuubi charm when a client application is related to it."""
    logger.info("Integrating data-integrator charm with kyuubi-k8s charm...")
    juju.integrate(APP_NAME, charm_versions.data_integrator.app)

    logger.info("Waiting for all charms to be idle and active...")
    juju.wait(jubilant.all_active, delay=10)

    # Fetch password for operator user from postgresql-k8s
    postgres_leader = get_leader_unit(juju, charm_versions.auth_db.app)
    status = juju.status()
    postgres_host = status.apps[charm_versions.auth_db.app].units[postgres_leader].address

    task = juju.run(postgres_leader, "get-password")
    assert task.return_code == 0
    password = task.results["password"]

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
        relation_username, hashed_relation_password = cursor.fetchone()  # type: ignore

    # A new user has indeed been created for data-integrator
    assert num_users == 1

    logger.info(f"Relation user's username: {relation_username}.")
    jdbc_uri, kyuubi_username, kyuubi_password = fetch_connection_info(
        juju, charm_versions.data_integrator.app
    )

    assert kyuubi_username == relation_username
    context["relation_username"] = relation_username
    context["relation_password"] = kyuubi_password
    context["jdbc_uri"] = jdbc_uri

    assert validate_sql_queries_with_kyuubi(
        juju=juju, jdbc_uri=jdbc_uri, username=kyuubi_username, password=kyuubi_password
    )


def test_kyuubi_client_relation_removed(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, context
) -> None:
    """Test the behavior of Kyuubi when client application relation is removed from it."""
    old_relation_username = context.pop("relation_username")
    old_relation_password = context.pop("relation_password")
    jdbc_uri = context.pop("jdbc_uri")

    logger.info("Removing relation between data-integrator and kyuubi-k8s...")
    juju.remove_relation(
        f"{APP_NAME}:{KYUUBI_CLIENT_RELATION_NAME}",
        f"{charm_versions.data_integrator.app}",
    )

    logger.info("Waiting for data-integrator and kyuubi charm to be idle and active...")
    status = juju.wait(
        lambda status: jubilant.all_blocked(status, charm_versions.data_integrator.app)
        and jubilant.all_active(status, APP_NAME),
        delay=3,
    )

    # Fetch password for operator user from postgresql-k8s
    postgres_leader = get_leader_unit(juju, charm_versions.auth_db.app)
    postgres_host = status.apps[charm_versions.auth_db.app].units[postgres_leader].address

    task = juju.run(postgres_leader, "get-password")
    assert task.return_code == 0
    password = task.results["password"]

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
            juju=juju,
            jdbc_uri=jdbc_uri,
            username=old_relation_password,
            password=old_relation_username,
        )
        assert b"Error validating the login" in getattr(exc.value, "message", b"")
