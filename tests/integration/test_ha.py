#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

# TODO: Revisit this test after recent updates in the purpose of Kyuubi <> Zookeeper relation

import logging
import time
from pathlib import Path
from typing import cast

import jubilant
import yaml

from core.domain import Status

from .helpers import (
    delete_pod,
    deploy_minimal_kyuubi_setup,
    fetch_jdbc_endpoint,
    fetch_password,
    get_active_kyuubi_servers_list,
    get_kyuubi_pid,
    is_entire_cluster_responding_requests,
    kill_kyuubi_process,
    run_sql_test_against_jdbc_endpoint,
)
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def test_build_and_deploy_cluster_with_no_zookeeper(
    juju: jubilant.Juju,
    kyuubi_charm: Path,
    charm_versions: IntegrationTestsCharms,
    s3_bucket_and_creds: S3Info,
) -> None:
    deploy_minimal_kyuubi_setup(
        juju=juju,
        kyuubi_charm=str(kyuubi_charm),
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=True,
    )

    # Wait for everything to settle down
    juju.wait(jubilant.all_active, delay=10)


def test_standalone_kyuubi_works_without_zookeeper(juju: jubilant.Juju, test_pod: str) -> None:
    username = "admin"
    password = fetch_password(juju)

    # Run SQL tests against JDBC endpoint
    jdbc_endpoint = fetch_jdbc_endpoint(juju)
    assert run_sql_test_against_jdbc_endpoint(
        juju, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint, username=username, password=password
    )


def test_scale_up_kyuubi_to_three_units_without_zookeeper(juju: jubilant.Juju) -> None:
    """Test scaling up action on Kyuubi."""
    # Scale Kyuubi charm to 3 units
    juju.add_unit(APP_NAME, num_units=2)
    status = juju.wait(lambda status: jubilant.all_blocked(status, APP_NAME), delay=10)

    assert len(status.apps[APP_NAME].units) == 3
    assert status.apps[APP_NAME].app_status.message == Status.MISSING_ZOOKEEPER.value.message


def test_zookeeper_relation_with_three_units_of_kyuubi(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, test_pod: str
) -> None:
    """Test relating Zookeeper with Kyuubi with multiple units."""
    # Deploy Zookeeper and wait
    juju.deploy(**charm_versions.zookeeper.deploy_dict())
    logger.info("Waiting for zookeeper-k8s charm to be active and idle...")
    juju.wait(lambda status: jubilant.all_active(status, charm_versions.zookeeper.app), delay=5)

    # Integrate Kyuubi and Zookeeper
    logger.info("Integrating kyuubi charm with zookeeper charm...")
    juju.integrate(charm_versions.zookeeper.application_name, APP_NAME)

    logger.info("Waiting for zookeeper-k8s and kyuubi charms to be active and idle...")
    juju.wait(jubilant.all_active, delay=10)

    active_servers = get_active_kyuubi_servers_list(
        juju=juju, zookeeper_name=charm_versions.zookeeper.app
    )
    assert len(active_servers) == 3

    expected_servers = [
        f"kyuubi-k8s-0.kyuubi-k8s-endpoints.{cast(str, juju.model)}.svc.cluster.local",
        f"kyuubi-k8s-1.kyuubi-k8s-endpoints.{cast(str, juju.model)}.svc.cluster.local",
        f"kyuubi-k8s-2.kyuubi-k8s-endpoints.{cast(str, juju.model)}.svc.cluster.local",
    ]
    assert set(active_servers) == set(expected_servers)

    # Run SQL test against the cluster
    username = "admin"
    password = fetch_password(juju)

    # Run SQL tests against JDBC endpoint
    jdbc_endpoint = fetch_jdbc_endpoint(juju)
    assert run_sql_test_against_jdbc_endpoint(
        juju, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint, username=username, password=password
    )

    # Assert the entire cluster is usable
    assert is_entire_cluster_responding_requests(
        juju, test_pod, username=username, password=password
    )


def test_pod_reschedule(
    juju: jubilant.Juju, test_pod: str, charm_versions: IntegrationTestsCharms
) -> None:
    """Test Kyuubi cluster after the leader pod is reschedule."""
    status = juju.status()
    leader_unit = None
    for name, unit in status.apps[APP_NAME].units.items():
        if unit.leader:
            leader_unit = name
    assert leader_unit
    leader_unit_pod = leader_unit.replace("/", "-")

    # Delete the leader pod
    delete_pod(leader_unit_pod, cast(str, juju.model))

    # let pod reschedule process be noticed up by juju
    status = juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=10)

    assert len(status.apps[APP_NAME].units) == 3

    active_servers = get_active_kyuubi_servers_list(
        juju=juju, zookeeper_name=charm_versions.zookeeper.app
    )
    assert len(active_servers) == 3

    # Run SQL test against the cluster
    username = "admin"
    password = fetch_password(juju)

    # Run SQL tests against JDBC endpoint
    jdbc_endpoint = fetch_jdbc_endpoint(juju)
    assert run_sql_test_against_jdbc_endpoint(
        juju, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint, username=username, password=password
    )

    # Assert the entire cluster is usable
    assert is_entire_cluster_responding_requests(
        juju, test_pod, username=username, password=password
    )


def test_kill_kyuubi_process(
    juju: jubilant.Juju, test_pod: str, charm_versions: IntegrationTestsCharms
) -> None:
    """Test Kyuubi cluster after Kyuubi process in the leader unit is killed with SIGKILL signal."""
    status = juju.status()
    leader_unit = None
    for name, unit in status.apps[APP_NAME].units.items():
        if unit.leader:
            leader_unit = name
    assert leader_unit

    # Get the current PID of Kyuubi process
    kyuubi_pid_old = get_kyuubi_pid(juju, leader_unit)
    assert kyuubi_pid_old is not None, f"No Kyuubi process found running in the unit {leader_unit}"

    # Kill Kyuubi process inside the leader unit
    kill_kyuubi_process(juju, leader_unit, kyuubi_pid_old)

    # Wait a few seconds for the process to re-appear
    time.sleep(10)

    # Get the new PID of Kyuubi process
    kyuubi_pid_new = get_kyuubi_pid(juju, leader_unit)
    assert kyuubi_pid_new is not None
    assert kyuubi_pid_new != kyuubi_pid_old

    # Ensure Kyuubi is in active and idle state
    status = juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=10)

    assert len(status.apps[APP_NAME].units) == 3

    active_servers = get_active_kyuubi_servers_list(
        juju=juju, zookeeper_name=charm_versions.zookeeper.app
    )
    assert len(active_servers) == 3

    # Run SQL test against the cluster
    username = "admin"
    password = fetch_password(juju)

    # Run SQL tests against JDBC endpoint
    jdbc_endpoint = fetch_jdbc_endpoint(juju)
    assert run_sql_test_against_jdbc_endpoint(
        juju, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint, username=username, password=password
    )

    # Assert the entire cluster is usable
    assert is_entire_cluster_responding_requests(
        juju, test_pod, username=username, password=password
    )


def test_scale_down_kyuubi_from_three_to_two_with_zookeeper(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, test_pod: str
) -> None:
    """Test scaling down action on Kyuubi."""
    # Scale Kyuubi charm to 3 units
    juju.remove_unit(APP_NAME, num_units=1)
    status = juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME, charm_versions.zookeeper.app),
        delay=10,
    )

    assert len(status.apps[APP_NAME].units) == 2

    active_servers = get_active_kyuubi_servers_list(
        juju=juju, zookeeper_name=charm_versions.zookeeper.application_name
    )
    assert len(active_servers) == 2

    # Run SQL test against the cluster
    username = "admin"
    password = fetch_password(juju)

    # Run SQL tests against JDBC endpoint
    jdbc_endpoint = fetch_jdbc_endpoint(juju)
    assert run_sql_test_against_jdbc_endpoint(
        juju, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint, username=username, password=password
    )

    # Assert the entire cluster is usable
    assert is_entire_cluster_responding_requests(
        juju, test_pod, username=username, password=password
    )


def test_scale_down_to_standalone_kyuubi_with_zookeeper(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, test_pod: str
) -> None:
    # Scale Kyuubi charm to 1 unit
    juju.remove_unit(APP_NAME, num_units=1)
    status = juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME, charm_versions.zookeeper.app),
        delay=10,
    )

    assert len(status.apps[APP_NAME].units) == 1

    active_servers = get_active_kyuubi_servers_list(
        juju=juju, zookeeper_name=charm_versions.zookeeper.application_name
    )
    assert len(active_servers) == 1

    # Run SQL test against the cluster
    username = "admin"
    password = fetch_password(juju)

    # Run SQL tests against JDBC endpoint
    jdbc_endpoint = fetch_jdbc_endpoint(juju)
    assert run_sql_test_against_jdbc_endpoint(
        juju, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint, username=username, password=password
    )

    # Assert the entire cluster is usable
    assert is_entire_cluster_responding_requests(
        juju, test_pod, username=username, password=password
    )


def test_remove_zookeeper_relation_on_single_unit(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, test_pod: str
) -> None:
    logger.info("Removing relation between zookeeper-k8s and kyuubi-k8s...")
    juju.remove_relation(
        f"{APP_NAME}:zookeeper", f"{charm_versions.zookeeper.application_name}:zookeeper"
    )

    juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME, charm_versions.zookeeper.app),
        delay=10,
    )

    # Run SQL test against the cluster
    username = "admin"
    password = fetch_password(juju)

    # Run SQL tests against JDBC endpoint
    jdbc_endpoint = fetch_jdbc_endpoint(juju)
    assert run_sql_test_against_jdbc_endpoint(
        juju, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint, username=username, password=password
    )
