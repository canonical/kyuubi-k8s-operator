#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import logging
import uuid
from pathlib import Path

import jubilant
import pytest
import yaml
from thrift.transport.TTransport import TTransportException

from core.domain import Status

from .helpers import (
    deploy_minimal_kyuubi_setup,
    fetch_connection_info,
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


def test_kyuubi_without_passing_credentials(juju: jubilant.Juju) -> None:
    """Test the JDBC connection when invalid credentials are provided."""
    with pytest.raises(TTransportException):
        validate_sql_queries_with_kyuubi(juju=juju)


def test_kyuubi_with_invalid_credentials(juju: jubilant.Juju) -> None:
    """Test the JDBC connection when invalid credentials are provided."""
    username = "admin"
    password = str(uuid.uuid4())
    with pytest.raises(TTransportException):
        validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)


def test_kyuubi_valid_credentials(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test the JDBC connection when valid credentials are provided."""
    logger.info("Running action 'get-password' on kyuubi unit")
    _, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    assert validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)


def test_set_admin_password_in_kyuubi_secret_not_granted(juju: jubilant.Juju) -> None:
    username = "admin"
    password = "password"
    secret_name = "admin-password-no-grant"
    secret_uri = juju.add_secret(secret_name, {username: password})
    juju.config(APP_NAME, {"system-users": secret_uri})
    juju.wait(jubilant.all_agents_idle)
    juju.wait(lambda status: jubilant.all_blocked(status, APP_NAME))
    status = juju.status()
    assert (
        status.apps[APP_NAME].app_status.message
        == Status.SYSTEM_USERS_SECRET_INSUFFICIENT_PERMISSION.value.message
    )
    with pytest.raises(TTransportException):
        validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)


def test_set_admin_password_in_kyuubi_secret_not_valid(juju: jubilant.Juju) -> None:
    username = "randomuser"
    password = "password"
    secret_name = "admin-password-invalid"
    secret_uri = juju.add_secret(secret_name, {username: password})
    juju.config(APP_NAME, {"system-users": secret_uri})
    juju.wait(jubilant.all_agents_idle)
    juju.wait(lambda status: jubilant.all_blocked(status, APP_NAME))
    status = juju.status()
    assert (
        status.apps[APP_NAME].app_status.message
        == Status.SYSTEM_USERS_SECRET_INVALID.value.message
    )
    with pytest.raises(TTransportException):
        validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)


def test_set_admin_password_in_kyuubi_secret_valid(juju: jubilant.Juju) -> None:
    username = "admin"
    password = "password"
    secret_name = "kyuubi-users"
    secret_uri = juju.add_secret(secret_name, {username: password})
    juju.config(APP_NAME, {"system-users": secret_uri})
    juju.wait(jubilant.all_agents_idle)
    juju.wait(jubilant.all_active)

    assert validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)


def test_update_admin_password(juju: jubilant.Juju) -> None:
    username = "admin"
    old_password = "password"
    new_password = "new-password"
    secret_name = "kyuubi-users"
    juju.cli("update-secret", secret_name, f"{username}={new_password}")
    juju.wait(jubilant.all_agents_idle)
    juju.wait(jubilant.all_active)
    with pytest.raises(TTransportException):
        validate_sql_queries_with_kyuubi(juju=juju, username=username, password=old_password)

    assert validate_sql_queries_with_kyuubi(juju=juju, username=username, password=new_password)


def test_update_admin_password_to_invalid_and_valid_secret_again(juju: jubilant.Juju) -> None:
    username = "random-user"
    password = "new-password"
    secret_name = "kyuubi-users"
    juju.cli("update-secret", secret_name, f"{username}={password}")
    juju.wait(jubilant.all_agents_idle)
    juju.wait(lambda status: jubilant.all_blocked(status, APP_NAME))
    status = juju.status()
    assert (
        status.apps[APP_NAME].app_status.message
        == Status.SYSTEM_USERS_SECRET_INVALID.value.message
    )
    with pytest.raises(TTransportException):
        validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)

    new_username = "admin"
    new_password = "valid-admin-password"
    juju.cli("update-secret", secret_name, f"{username}={password}")
    juju.wait(jubilant.all_agents_idle)
    juju.wait(jubilant.all_active)
    with pytest.raises(TTransportException):
        validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)

    assert validate_sql_queries_with_kyuubi(
        juju=juju, username=new_username, password=new_password
    )


def test_remove_admin_password_config(juju: jubilant.Juju, charm_versions) -> None:
    juju.config(APP_NAME, {"system-users": ""})
    juju.wait(jubilant.all_agents_idle)
    juju.wait(jubilant.all_active)

    old_username = "admin"
    old_password = "valid-admin-password"
    with pytest.raises(TTransportException):
        validate_sql_queries_with_kyuubi(juju=juju, username=old_username, password=old_password)

    _, new_username, new_password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    assert validate_sql_queries_with_kyuubi(
        juju=juju, username=new_username, password=new_password
    )


def test_remove_authentication_database(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test the workload is stopped when authentication is disabled."""
    logger.info("Removing relation between postgresql-k8s and kyuubi-k8s over auth-db endpoint...")
    juju.remove_relation(
        f"{APP_NAME}:auth-db", f"{charm_versions.auth_db.application_name}:database"
    )

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    juju.wait(
        lambda status: jubilant.all_blocked(status, APP_NAME),
        delay=3,
    )

    with pytest.raises(TTransportException):
        validate_sql_queries_with_kyuubi(juju=juju)
