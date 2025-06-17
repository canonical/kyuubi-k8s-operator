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


# test set admin password secret does not exist
# test set admin password secret not granted
# test set admin password invalid secret
# test set admin password valid secret, assert admin password can be used to connect to kyuubi
# update admin password, now the old password should not work, new password should work
# make the secret invalid, charm should revert back to blocked state
# make the secret valid again, the charm should revert back to active state
# remove the admin password secret, the password should no longer work (however charm does not know of this.. so IDK how that will fare?)
# Even when admin password secret is removed, one can still connect with data-intregrator user


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
