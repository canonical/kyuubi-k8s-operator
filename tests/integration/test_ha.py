#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

# TODO: Revisit this test after recent updates in the purpose of Kyuubi <> Zookeeper relation

import logging
from pathlib import Path

import pytest
import yaml
from juju.application import Application
from juju.unit import Unit
from ops import StatusBase
from pytest_operator.plugin import OpsTest

from core.domain import Status

from .helpers import (
    delete_pod,
    deploy_minimal_kyuubi_setup,
    find_leader_unit,
    get_active_kyuubi_servers_list,
    get_kyuubi_pid,
    is_entire_cluster_responding_requests,
    juju_sleep,
    kill_kyuubi_process,
    run_sql_test_against_jdbc_endpoint,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
TEST_CHARM_PATH = "./tests/integration/app-charm"
TEST_CHARM_NAME = "application"


def check_status(entity: Application | Unit, status: StatusBase):
    if isinstance(entity, Application):
        return entity.status == status.name and entity.status_message == status.message
    elif isinstance(entity, Unit):
        return (
            entity.workload_status == status.name
            and entity.workload_status_message == status.message
        )
    else:
        raise ValueError(f"entity type {type(entity)} is not allowed")


@pytest.mark.abort_on_fail
async def test_build_and_deploy_cluster_with_no_zookeeper(
    ops_test: OpsTest, kyuubi_charm, charm_versions, s3_bucket_and_creds
):
    await deploy_minimal_kyuubi_setup(
        ops_test=ops_test,
        kyuubi_charm=kyuubi_charm,
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=True,
    )

    # Wait for everything to settle down
    await ops_test.model.wait_for_idle(
        apps=[
            APP_NAME,
            charm_versions.integration_hub.application_name,
            charm_versions.s3.application_name,
        ],
        idle_period=20,
        status="active",
    )

    assert check_status(ops_test.model.applications[APP_NAME], Status.ACTIVE.value)
    assert ops_test.model.applications[charm_versions.s3.application_name].status == "active"
    assert (
        ops_test.model.applications[charm_versions.integration_hub.application_name].status
        == "active"
    )


@pytest.mark.abort_on_fail
async def test_standalone_kyuubi_works_without_zookeeper(ops_test: OpsTest, test_pod):
    assert await run_sql_test_against_jdbc_endpoint(ops_test, test_pod)


@pytest.mark.abort_on_fail
async def test_scale_up_kyuubi_to_three_units_without_zookeeper(
    ops_test: OpsTest, charm_versions, test_pod
):
    """Test scaling up action on Kyuubi."""
    # Scale Kyuubi charm to 3 units
    await ops_test.model.applications[APP_NAME].scale(scale=3)
    await ops_test.model.block_until(lambda: len(ops_test.model.applications[APP_NAME].units) == 3)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], timeout=1000, idle_period=30, status="blocked"
    )

    assert len(ops_test.model.applications[APP_NAME].units) == 3
    assert check_status(ops_test.model.applications[APP_NAME], Status.MISSING_ZOOKEEPER.value)


@pytest.mark.abort_on_fail
async def test_zookeeper_relation_with_three_units_of_kyuubi(
    ops_test: OpsTest, charm_versions, test_pod
):
    """Test relating Zookeeper with Kyuubi with multiple units."""
    # Deploy Zookeeper and wait
    await ops_test.model.deploy(**charm_versions.zookeeper.deploy_dict())
    logger.info("Waiting for zookeeper-k8s charm to be active and idle...")
    await ops_test.model.wait_for_idle(
        apps=[charm_versions.zookeeper.application_name],
        timeout=1000,
        idle_period=20,
        status="active",
    )

    # Integrate Kyuubi and Zookeeper
    logger.info("Integrating kyuubi charm with zookeeper charm...")
    await ops_test.model.integrate(charm_versions.zookeeper.application_name, APP_NAME)

    logger.info("Waiting for zookeeper-k8s and kyuubi charms to be active and idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.zookeeper.application_name], timeout=1000, status="active"
    )

    # Assert that all charms are in active and idle state
    assert check_status(ops_test.model.applications[APP_NAME], Status.ACTIVE.value)
    assert ops_test.model.applications[charm_versions.s3.application_name].status == "active"
    assert (
        ops_test.model.applications[charm_versions.integration_hub.application_name].status
        == "active"
    )
    assert (
        ops_test.model.applications[charm_versions.zookeeper.application_name].status == "active"
    )

    active_servers = await get_active_kyuubi_servers_list(
        ops_test=ops_test, zookeeper_name=charm_versions.zookeeper.application_name
    )
    assert len(active_servers) == 3

    expected_servers = [
        f"kyuubi-k8s-0.kyuubi-k8s-endpoints.{ops_test.model_name}.svc.cluster.local",
        f"kyuubi-k8s-1.kyuubi-k8s-endpoints.{ops_test.model_name}.svc.cluster.local",
        f"kyuubi-k8s-2.kyuubi-k8s-endpoints.{ops_test.model_name}.svc.cluster.local",
    ]
    assert set(active_servers) == set(expected_servers)

    # Run SQL test against the cluster
    assert await run_sql_test_against_jdbc_endpoint(ops_test, test_pod)

    # Assert the entire cluster is usable
    assert await is_entire_cluster_responding_requests(ops_test, test_pod)


async def test_pod_reschedule(ops_test: OpsTest, test_pod, charm_versions):
    """Test Kyuubi cluster after the leader pod is reschedule."""
    leader_unit = await find_leader_unit(ops_test, APP_NAME)
    leader_unit_pod = leader_unit.name.replace("/", "-")

    # Delete the leader pod
    await delete_pod(leader_unit_pod, ops_test.model_name)

    # let pod reschedule process be noticed up by juju
    async with ops_test.fast_forward("60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], idle_period=30, status="active", timeout=1000
        )

    assert len(ops_test.model.applications[APP_NAME].units) == 3

    active_servers = await get_active_kyuubi_servers_list(
        ops_test=ops_test, zookeeper_name=charm_versions.zookeeper.application_name
    )
    assert len(active_servers) == 3

    # Run SQL test against the cluster
    assert await run_sql_test_against_jdbc_endpoint(ops_test, test_pod)

    # Assert the entire cluster is usable
    assert await is_entire_cluster_responding_requests(ops_test, test_pod)


async def test_kill_kyuubi_process(ops_test: OpsTest, test_pod, charm_versions):
    """Test Kyuubi cluster after Kyuubi process in the leader unit is killed with SIGKILL signal."""
    leader_unit = await find_leader_unit(ops_test, APP_NAME)

    # Get the current PID of Kyuubi process
    kyuubi_pid_old = await get_kyuubi_pid(ops_test, leader_unit)
    assert kyuubi_pid_old is not None, (
        f"No Kyuubi process found running in the unit {leader_unit.name}"
    )

    # Kill Kyuubi process inside the leader unit
    await kill_kyuubi_process(ops_test, leader_unit, kyuubi_pid_old)

    # Wait a few seconds for the process to re-appear
    await juju_sleep(ops_test, 10, APP_NAME)

    # Get the new PID of Kyuubi process
    kyuubi_pid_new = await get_kyuubi_pid(ops_test, leader_unit)
    assert kyuubi_pid_new is not None
    assert kyuubi_pid_new != kyuubi_pid_old

    # Ensure Kyuubi is in active and idle state
    async with ops_test.fast_forward("10s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], idle_period=30, status="active", timeout=1000
        )

    assert len(ops_test.model.applications[APP_NAME].units) == 3

    active_servers = await get_active_kyuubi_servers_list(
        ops_test=ops_test, zookeeper_name=charm_versions.zookeeper.application_name
    )
    assert len(active_servers) == 3

    # Run SQL test against the cluster
    assert await run_sql_test_against_jdbc_endpoint(ops_test, test_pod)

    # Assert the entire cluster is usable
    assert await is_entire_cluster_responding_requests(ops_test, test_pod)


@pytest.mark.abort_on_fail
async def test_scale_down_kyuubi_from_three_to_two_with_zookeeper(
    ops_test: OpsTest, charm_versions, test_pod
):
    """Test scaling down action on Kyuubi."""
    # Scale Kyuubi charm to 3 units
    await ops_test.model.applications[APP_NAME].scale(scale=2)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=1000, idle_period=30, wait_for_exact_units=2
    )
    await ops_test.model.wait_for_idle(
        apps=[charm_versions.zookeeper.application_name],
        status="active",
        timeout=1000,
        idle_period=30,
    )

    assert len(ops_test.model.applications[APP_NAME].units) == 2

    active_servers = await get_active_kyuubi_servers_list(
        ops_test=ops_test, zookeeper_name=charm_versions.zookeeper.application_name
    )
    assert len(active_servers) == 2

    # Run SQL test against the cluster
    assert await run_sql_test_against_jdbc_endpoint(ops_test, test_pod)

    # Assert the entire cluster is usable
    assert await is_entire_cluster_responding_requests(ops_test, test_pod)


@pytest.mark.abort_on_fail
async def test_scale_down_to_standalone_kyuubi_with_zookeeper(
    ops_test: OpsTest, charm_versions, test_pod
):
    # Scale Kyuubi charm to 1 unit
    await ops_test.model.applications[APP_NAME].scale(scale=1)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=1000, idle_period=30, wait_for_exact_units=1
    )
    await ops_test.model.wait_for_idle(
        apps=[charm_versions.zookeeper.application_name],
        status="active",
        timeout=1000,
        idle_period=30,
    )

    assert len(ops_test.model.applications[APP_NAME].units) == 1

    active_servers = await get_active_kyuubi_servers_list(
        ops_test=ops_test, zookeeper_name=charm_versions.zookeeper.application_name
    )
    assert len(active_servers) == 1

    # Run SQL test against the cluster
    assert await run_sql_test_against_jdbc_endpoint(ops_test, test_pod)

    # Assert the entire cluster is usable
    assert await is_entire_cluster_responding_requests(ops_test, test_pod)


@pytest.mark.abort_on_fail
async def test_remove_zookeeper_relation_on_single_unit(
    ops_test: OpsTest, charm_versions, test_pod
):
    logger.info("Removing relation between zookeeper-k8s and kyuubi-k8s...")
    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:zookeeper", f"{charm_versions.zookeeper.application_name}:zookeeper"
    )

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=1000, idle_period=30, wait_for_exact_units=1
    )
    await ops_test.model.wait_for_idle(
        apps=[charm_versions.zookeeper.application_name],
        status="active",
        timeout=1000,
        idle_period=30,
    )

    # Run SQL test against the standalone Kyuubi
    assert await run_sql_test_against_jdbc_endpoint(ops_test, test_pod)
