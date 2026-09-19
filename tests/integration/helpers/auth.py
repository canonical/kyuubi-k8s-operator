# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from __future__ import annotations

import logging
from pathlib import Path

import jubilant
import yaml

from ..types import IntegrationTestsCharms

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
ZOOKEEPER_NAME = "zookeeper-k8s"
ZOOKEEPER_PORT = 2181

PROCESS_NAME_PATTERN = "org.apache.kyuubi.server.KyuubiServer"
KYUUBI_CONTAINER_NAME = "kyuubi"

NODEPORT_MIN_VALUE = 30000
NODEPORT_MAX_VALUE = 32767
JDBC_PORT = 10009
JDBC_PORT_NAME = "kyuubi-jdbc"

SAMPLE_USERS_LDIF = Path("./tests/integration/setup/sample-ldap-users.ldif")
LDAP_TEST_USERNAME = "bikalpa"
LDAP_TEST_USER_EMAIL = "bikalpa@glauth.com"
LDAP_TEST_USER_CUSTOM_ID = "dhakal"
LDAP_TEST_PASSWORD = "bikalpa"

LATEST_STABLE_REV = 112


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


def setup_jdbc_authentication(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    """Setup a minimal deployment for JDBC authentication with Kyuubi."""
    logger.info("Deploying postgresql-k8s charm as auth-db...")

    juju.deploy(**charm_versions.auth_db.deploy_dict())

    logger.info("Waiting for auth-db charm to be idle and active...")
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.auth_db.app,
        ),
        delay=15,
        timeout=2000,
    )
    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm...")
    juju.integrate(charm_versions.auth_db.application_name, f"{APP_NAME}:auth-db")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.auth_db.app,
        ),
        delay=15,
        timeout=1000,
    )
    logger.info("JDBC authentication setup completed.")


def setup_ldap_authentication(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    """Setup a minimal deployment for LDAP authentication with Kyuubi."""
    logger.info(
        "Deploying glauth-k8s charm as ldap provider, postsgresql-k8s as users store, and glauth-utils to manage glauth..."
    )
    juju.deploy(**charm_versions.glauth.deploy_dict())
    juju.deploy(**charm_versions.glauth_utils.deploy_dict())
    juju.deploy(**charm_versions.ldap_db.deploy_dict())

    logger.info("Deploying self-signed-certificates as TLS provider...")
    juju.deploy(**charm_versions.ldap_tls.deploy_dict())

    logger.info("Integrating glauth charm with postgresql-k8s charm...")
    juju.integrate(
        f"{charm_versions.glauth.application_name}:pg-database",
        f"{charm_versions.ldap_db.application_name}:database",
    )

    logger.info("Integrating glauth charm with certificates provider...")
    juju.integrate(
        f"{charm_versions.glauth.application_name}:certificates",
        f"{charm_versions.ldap_tls.application_name}:certificates",
    )

    logger.info("Integrating glauth-utils charm with glauth charm...")
    juju.integrate(
        f"{charm_versions.glauth_utils.application_name}:glauth-auxiliary",
        f"{charm_versions.glauth.application_name}:glauth-auxiliary",
    )

    logger.info("Waiting for auth charms to be active and idle...")
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.ldap_db.app,
            charm_versions.glauth.app,
            charm_versions.glauth_utils.app,
            charm_versions.ldap_tls.app,
        ),
        delay=15,
        timeout=1000,
    )
    logger.info("Enable LDAPS in glauth-k8s charm...")
    juju.config(charm_versions.glauth.application_name, values={"ldaps_enabled": "true"})
    logger.info(
        "Integrating kyuubi-k8s charm with glauth charm (over ldap and send-ca-cert interfaces)..."
    )
    juju.integrate(
        f"{charm_versions.glauth.application_name}:ldap", f"{APP_NAME}:ldap-credentials"
    )
    juju.integrate(
        f"{charm_versions.glauth.application_name}:send-ca-cert", f"{APP_NAME}:receive-ca-cert"
    )

    logger.info("Waiting for all charms to be active and idle...")
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.ldap_db.app,
            charm_versions.glauth.app,
            charm_versions.glauth_utils.app,
            charm_versions.ldap_tls.app,
            APP_NAME,
        ),
        delay=15,
        timeout=1000,
    )
    apply_sample_users_ldif(juju=juju, charm_versions=charm_versions)
    logger.info("LDAP authentication setup completed.")
