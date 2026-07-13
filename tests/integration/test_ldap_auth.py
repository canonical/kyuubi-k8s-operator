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
    validate_sql_queries_with_kyuubi,
)
from .types import IntegrationTestsCharms

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

SAMPLE_USERS_LDIF = Path("./tests/integration/setup/sample_users.ldif")
LDAP_TEST_USER = "bikalpa"
LDAP_TEST_PASSWORD = "bikalpa"


def apply_sample_users_ldif(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    """Apply a sample LDIF file to the glauth-k8s charm to create users for LDAP authentication."""
    if not SAMPLE_USERS_LDIF.exists():
        raise FileNotFoundError(f"Sample LDIF file not found: {SAMPLE_USERS_LDIF}")
    sample_ldif_file = SAMPLE_USERS_LDIF

    logger.info("Applying sample LDIF file to glauth-k8s...")
    juju.scp(
        str(sample_ldif_file),
        f"{charm_versions.glauth_utils.application_name}/0:/tmp/sample_users.ldif",
    )
    result = juju.run(
        f"{charm_versions.glauth_utils.application_name}/0",
        "apply-ldif",
        {
            "path": "/tmp/sample_users.ldif",
        },
    )
    assert result.return_code == 0, f"Failed to apply sample LDIF file: {result.stderr}"
    logger.info("Sample LDIF file applied successfully.")


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
        auth_mode="ldap",
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


def test_create_ldap_users(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    apply_sample_users_ldif(juju=juju, charm_versions=charm_versions)


def test_kyuubi_valid_credentials(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test the JDBC connection when valid credentials are provided."""
    username, password = LDAP_TEST_USER, LDAP_TEST_PASSWORD
    assert validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)


def test_remove_ldap_relation(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    """Test the behavior of the charm when ldap relation is removed."""
    juju.remove_relation(f"{APP_NAME}:ldap-credentials", charm_versions.glauth.app)
    juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_blocked(status, APP_NAME),
        delay=5,
    )

    username, password = LDAP_TEST_USER, LDAP_TEST_PASSWORD
    with pytest.raises(TTransportException):
        validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)


def test_ldap_relation_integrated_again(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test the workload is stopped when authentication is disabled."""
    juju.integrate(f"{APP_NAME}:ldap-credentials", charm_versions.glauth.app)
    juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status, APP_NAME),
        delay=5,
    )

    username, password = LDAP_TEST_USER, LDAP_TEST_PASSWORD
    assert validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)
