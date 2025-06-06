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
    get_leader_unit,
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
    """Test the JDBC connection when invalid credentials are provided."""
    logger.info("Running action 'get-password' on kyuubi unit")
    _, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    assert validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)


def test_set_password_action(juju: jubilant.Juju) -> None:
    """Test set-password action."""
    logger.info("Running action 'set-password' on kyuubi-k8s unit...")
    new_password = str(uuid.uuid4())
    leader = get_leader_unit(juju, APP_NAME)
    task = juju.run(leader, "set-password", {"password": new_password})
    assert task.return_code == 0

    logger.info("Running action 'get-password' on kyuubi unit")
    task = juju.run(leader, "get-password")
    assert task.return_code == 0
    assert new_password == task.results["password"]

    username = "admin"
    assert validate_sql_queries_with_kyuubi(juju=juju, username=username, password=new_password)


def test_remove_authentication(
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
