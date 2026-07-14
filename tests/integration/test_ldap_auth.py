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
    LDAP_TEST_PASSWORD,
    LDAP_TEST_USER,
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


def test_ldaps_disabled(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    """Test the behavior of the charm when LDAPS is disabled."""
    juju.config(charm_versions.glauth.application_name, {"ldaps_enabled": "false"})
    status = juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_blocked(status, APP_NAME),
        delay=5,
    )
    assert (
        status.apps[APP_NAME].app_status.message == Status.LDAP_CONNECTION_NOT_SECURE.value.message
    )

    username, password = LDAP_TEST_USER, LDAP_TEST_PASSWORD
    with pytest.raises(TTransportException):
        validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)


def test_reenable_ldaps(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    """Test the behavior of the charm when LDAPS is re-enabled."""
    juju.config(charm_versions.glauth.application_name, {"ldaps_enabled": "true"})
    status = juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=5,
    )
    assert status.apps[APP_NAME].app_status.message == Status.ACTIVE.value.message

    username, password = LDAP_TEST_USER, LDAP_TEST_PASSWORD
    assert validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)


def test_remove_certificate_transfer_relation(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test what happens when certificate_transfer relation between Kyuubi <> GlAuth is removed.

    In this case, the JDBC connection should fail, since Kyuubi cannot establish secure LDAP connection to GlAuth.
    """
    juju.remove_relation(f"{APP_NAME}:receive-ca-cert", charm_versions.glauth.app)
    status = juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=5,
    )
    assert status.apps[APP_NAME].app_status.message == Status.ACTIVE.value.message

    username, password = LDAP_TEST_USER, LDAP_TEST_PASSWORD
    with pytest.raises(TTransportException):
        validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)


def test_reintegrate_certificate_transfer_relation(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test what happens when certificate_transfer relation between Kyuubi <> GlAuth is re-integrated.

    In this case, the JDBC connection should succeed, since Kyuubi can establish secure LDAP connection to GlAuth.
    """
    juju.integrate(f"{APP_NAME}:receive-ca-cert", charm_versions.glauth.app)
    status = juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=5,
    )
    assert status.apps[APP_NAME].app_status.message == Status.ACTIVE.value.message

    username, password = LDAP_TEST_USER, LDAP_TEST_PASSWORD
    assert validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)
