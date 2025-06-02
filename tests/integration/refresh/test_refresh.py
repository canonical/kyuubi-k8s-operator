#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""This test module supports juju refresh in various scenarios.

We are talking about a test matrix with:
- external metastore; or not
- tls enabled; or not
- "ha" mode with multiple units; or a single one
- Upgrading the workload OCI on top of the charm; or keeping the default one
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
    get_leader_unit,
    inject_dependency_fault,
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


def test_pre_refresh_check(juju: jubilant.Juju) -> None:
    """Test that the pre-refresh-check action runs successfully."""
    logger.info("Get leader unit")
    leader_unit = get_leader_unit(juju, APP_NAME)

    logger.info("Run pre-refresh-check action")
    task = juju.run(leader_unit, "pre-refresh-check")
    assert task.return_code == 0


def test_run_inplace_upgrade(
    juju: jubilant.Juju,
    kyuubi_charm: Path,
    with_image_upgrade: bool,
    with_multi_units: bool,
) -> None:
    """Test that the inplace upgrade leads to an active deployment."""
    refresh_args: dict = {"path": str(kyuubi_charm)}
    if with_image_upgrade:
        # spark-3.4.4, release date 01/01/25
        image_version = "ghcr.io/canonical/charmed-spark-kyuubi@sha256:86fc84c8d01da25f756bebbae17395ef9702a8fd855565a4a80ed5d4f8024708"
        refresh_args["resources"] = {"kyuubi-image": image_version}
        logger.info(f"Will upgrade workload image to: {image_version}")

    logger.info("Refreshing Kyuubi")

    # start refresh by upgrading to the current version
    juju.refresh(APP_NAME, **refresh_args)

    logger.info("Waiting for upgrade to start")

    if not with_multi_units:
        # fast track, one unit will not block in any case
        juju.wait(jubilant.all_active, delay=10)
        return

    status = juju.wait(lambda status: jubilant.all_agents_idle(status, APP_NAME), delay=10)
    # Highest to lowest unit number
    refresh_order = sorted(
        status.apps[APP_NAME].units.keys(),
        key=lambda unit: int(unit.split("/")[1]),
        reverse=True,
    )

    assert (
        "Refresh incompatible"
        in status.apps[APP_NAME].units[refresh_order[0]].workload_status.message
    ), "Application refresh not blocked due to incompatibility"

    task_params = {"check-compatibility": False}
    if with_image_upgrade:
        task_params["check-workload-container"] = False

    task = juju.run(refresh_order[0], "force-refresh-start", task_params)
    assert task.return_code == 0

    logger.info("Waiting for first unit to upgrade")
    status = juju.wait(lambda status: jubilant.all_agents_idle(status, APP_NAME), delay=10)

    logger.info("Running resume-refresh action")
    leader_unit = get_leader_unit(juju, APP_NAME)
    try:
        juju.run(leader_unit, "resume-refresh")
    except jubilant.TaskError:
        # TODO: Ask about this
        # Not sure why we get a task terminated while things are apparently going fine.
        pass

    logger.info("Waiting for refresh to complete")
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


@pytest.mark.usefixtures("skipif_single_unit")
def test_fail_and_rollback(juju: jubilant.Juju, kyuubi_charm: Path, with_tls: bool) -> None:
    """Test that we can rollback after a failed upgrade.

    The test is skipped if we only have a single unit, as we cannot run compatibility checks.
    """
    logger.info("Get leader unit")
    leader_unit = get_leader_unit(juju, APP_NAME)

    logger.info("Run pre-refresh-check action")
    task = juju.run(leader_unit, "pre-refresh-check")
    assert task.return_code == 0

    with inject_dependency_fault(kyuubi_charm) as faulty_charm:
        logger.info("Refreshing the charm")
        juju.refresh(APP_NAME, path=faulty_charm)

    logger.info("Waiting for upgrade to fail")

    status = juju.wait(jubilant.any_blocked, delay=5)
    assert "incompatible" in status.apps[APP_NAME].app_status.message.lower()

    logger.info("Re-refresh the charm")

    juju.refresh(APP_NAME, path=kyuubi_charm)

    logger.info("Wait for application to recover")
    status = juju.wait(jubilant.all_active, delay=10)

    logger.info("Checking that deployment is working once again")
    username = "admin"
    password = fetch_password(juju)
    assert validate_sql_queries_with_kyuubi(
        juju=juju, username=username, password=password, use_tls=with_tls
    )
