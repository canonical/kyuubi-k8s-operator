#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""This test module supports testing inplace upgrades in various scenarios.

We are talking about a test matrix with:
- external metastore; or not
- tls enabled; or not
- "ha" mode with multiple units; or a single one
- Upgrading the workload OCI on top of the charm; or keeping the same one
"""

import logging
from pathlib import Path

import jubilant
import pytest
import yaml

from integration.helpers import (
    APP_NAME,
    deploy_minimal_kyuubi_setup,
    fetch_connection_info,
    validate_sql_queries_with_kyuubi,
)
from integration.types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

DB_NAME = "inplace_db"
TABLE_NAME = "inplace_table"
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())


def test_deploy(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    s3_bucket_and_creds: S3Info,
    with_multi_units: bool,
    with_tls: bool,
    with_metastore: bool,
):
    """Initial deployment.

    Take care of setting the necessary integrations depending on the current test scenario.
    """
    num_units = 1
    integrate_zookeeper = False

    if with_multi_units:
        num_units = 3
        integrate_zookeeper = True

    deploy_minimal_kyuubi_setup(
        juju=juju,
        kyuubi_charm="kyuubi-k8s",
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=True,
        num_units=num_units,
        integrate_zookeeper=integrate_zookeeper,
    )

    juju.wait(jubilant.all_active, delay=5)

    logger.info("Minimal deployment is ok")

    if with_tls:
        logger.info("Deploying self-signed-certs charm...")
        juju.deploy(
            **charm_versions.tls.deploy_dict(),
            config={"ca-common-name": "kyuubi"},
        )
        juju.wait(lambda status: jubilant.all_active(status, charm_versions.tls.app), delay=5)
        juju.integrate(APP_NAME, charm_versions.tls.app)

    if with_metastore:
        logger.info("Deploying postgresql-k8s charm...")
        juju.deploy(**charm_versions.metastore_db.deploy_dict())

        logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
        juju.wait(
            lambda status: jubilant.all_active(status, charm_versions.metastore_db.app),
            delay=10,
            timeout=2000,
        )

        logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm...")
        juju.integrate(charm_versions.metastore_db.app, f"{APP_NAME}:metastore-db")

    logger.info("Waiting for final deployment to be ok")
    juju.wait(jubilant.all_active, delay=15, timeout=1000)


def test_populate(
    juju: jubilant.Juju, with_tls: bool, charm_versions: IntegrationTestsCharms
) -> None:
    """Populate the database.

    We will use this to assert that we can still query data written prior to the inplace upgrade.
    """
    _, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    assert validate_sql_queries_with_kyuubi(
        juju=juju,
        db_name=DB_NAME,
        table_name=TABLE_NAME,
        username=username,
        password=password,
        use_tls=with_tls,
    )


def test_run_inplace_upgrade(
    juju: jubilant.Juju, kyuubi_charm: Path, with_image_upgrade: bool
) -> None:
    """Test that the inplace upgrade leads to an active deployment."""
    if not with_image_upgrade:
        image_version = METADATA["resources"]["kyuubi-image"]["upstream-source"]
    else:
        # spark-3.4.4, release date 01/01/25
        image_version = "ghcr.io/canonical/charmed-spark-kyuubi@sha256:86fc84c8d01da25f756bebbae17395ef9702a8fd855565a4a80ed5d4f8024708"

    logger.info(f"Image version: {image_version}")

    status = juju.status()
    leader_unit = None
    for name, unit in status.apps[APP_NAME].units.items():
        if unit.leader:
            leader_unit = name
    assert leader_unit

    # TODO trigger pre-upgrade checks after the release of the first charm with the upgrade feature available.

    # test upgrade procedure
    logger.info("Upgrading Kyuubi...")

    # start refresh by upgrading to the current version
    juju.refresh(
        APP_NAME,
        path=kyuubi_charm,
        # revision=49,
        resources={"kyuubi-image": image_version},
    )

    status = juju.wait(lambda status: jubilant.all_agents_idle(status, APP_NAME), delay=10)

    logger.info("Resume upgrade...")
    leader_unit = None
    for name, unit in status.apps[APP_NAME].units.items():
        if unit.leader:
            leader_unit = name
    assert leader_unit
    try:
        juju.run(leader_unit, "resume-upgrade")
    except Exception:
        pass

    juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=10)


def test_create_new_data(
    juju: jubilant.Juju, with_tls: bool, charm_versions: IntegrationTestsCharms
) -> None:
    """Test that the upgraded deployment is valid (can connect with auth, and write)."""
    _, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    assert validate_sql_queries_with_kyuubi(
        juju=juju, username=username, password=password, use_tls=with_tls
    )


@pytest.mark.usefixtures("skipif_no_metastore")
def test_validate_previous_data(
    juju: jubilant.Juju, with_tls: bool, charm_versions: IntegrationTestsCharms
) -> None:
    """Test that we can still access data from before the upgrade.

    This test is skipped if we were relying on the local metastore, since it would be gone.
    """
    _, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    assert validate_sql_queries_with_kyuubi(
        juju=juju,
        username=username,
        password=password,
        query_lines=[
            f"USE {DB_NAME};",
            f"SELECT * FROM {TABLE_NAME};",
        ],
        use_tls=with_tls,
    )
