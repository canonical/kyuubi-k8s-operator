#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import pytest
import yaml
from juju.errors import JujuUnitError

from core.domain import Status

from .helpers import (
    assert_service_status,
    check_status,
    deploy_minimal_kyuubi_setup,
    fetch_jdbc_endpoint,
    is_entire_cluster_responding_requests,
    run_sql_test_against_jdbc_endpoint,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
TEST_CHARM_PATH = "./tests/integration/app-charm"
TEST_CHARM_NAME = "application"


@pytest.mark.abort_on_fail
async def test_default_deploy(
    ops_test,
    kyuubi_charm,
    charm_versions,
    s3_bucket_and_creds,
    test_pod,
):
    """Test the status of default managed K8s service when Kyuubi is deployed."""
    await deploy_minimal_kyuubi_setup(
        ops_test=ops_test,
        kyuubi_charm=kyuubi_charm,
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=True,
        num_units=3,
        integrate_zookeeper=True,
    )

    # Wait for everything to settle down
    await ops_test.model.wait_for_idle(
        apps=[
            APP_NAME,
            charm_versions.integration_hub.application_name,
            charm_versions.zookeeper.application_name,
            charm_versions.s3.application_name,
        ],
        idle_period=20,
        status="active",
    )

    # Assert that all charms that were deployed as part of minimal setup are in correct states.
    assert check_status(ops_test.model.applications[APP_NAME], Status.ACTIVE.value)
    assert (
        ops_test.model.applications[charm_versions.integration_hub.application_name].status
        == "active"
    )
    assert ops_test.model.applications[charm_versions.s3.application_name].status == "active"
    assert (
        ops_test.model.applications[charm_versions.zookeeper.application_name].status == "active"
    )

    # Ensure that Kyuubi is exposed with ClusterIP service
    assert_service_status(namespace=ops_test.model_name, service_type="ClusterIP")

    # Run SQL tests against JDBC endpoint
    jdbc_endpoint = await fetch_jdbc_endpoint(ops_test)
    assert await run_sql_test_against_jdbc_endpoint(
        ops_test, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint
    )
    assert await is_entire_cluster_responding_requests(ops_test=ops_test, test_pod=test_pod)


@pytest.mark.abort_on_fail
async def test_nodeport_service(
    ops_test,
    test_pod,
):
    """Test the status of managed K8s service when `expose-external` is set to 'nodeport'."""
    logger.info("Changing expose-external to 'nodeport' for kyuubi-k8s charm...")
    await ops_test.model.applications[APP_NAME].set_config({"expose-external": "nodeport"})

    logger.info("Waiting for kyuubi-k8s app to be active and idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )

    assert_service_status(namespace=ops_test.model_name, service_type="NodePort")

    jdbc_endpoint = await fetch_jdbc_endpoint(ops_test)
    assert await run_sql_test_against_jdbc_endpoint(
        ops_test, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint
    )
    assert await is_entire_cluster_responding_requests(ops_test=ops_test, test_pod=test_pod)


@pytest.mark.abort_on_fail
async def test_loadbalancer_service(
    ops_test,
    test_pod,
):
    """Test the status of managed K8s service when `expose-external` is set to 'loadbalancer'."""
    logger.info("Changing expose-external to 'nodeport' for kyuubi-k8s charm...")
    await ops_test.model.applications[APP_NAME].set_config({"expose-external": "loadbalancer"})

    logger.info("Waiting for kyuubi-k8s app to be active and idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )

    assert_service_status(namespace=ops_test.model_name, service_type="LoadBalancer")

    jdbc_endpoint = await fetch_jdbc_endpoint(ops_test)
    assert await run_sql_test_against_jdbc_endpoint(
        ops_test, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint
    )
    assert await is_entire_cluster_responding_requests(ops_test=ops_test, test_pod=test_pod)


@pytest.mark.abort_on_fail
async def test_clusterip_service(
    ops_test,
    test_pod,
):
    """Test the status of managed K8s service when `expose-external` is set to 'false'."""
    logger.info("Changing expose-external to 'false' for kyuubi-k8s charm...")
    await ops_test.model.applications[APP_NAME].set_config({"expose-external": "false"})

    logger.info("Waiting for kyuubi-k8s app to be active and idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )

    assert_service_status(namespace=ops_test.model_name, service_type="ClusterIP")

    jdbc_endpoint = await fetch_jdbc_endpoint(ops_test)
    assert await run_sql_test_against_jdbc_endpoint(
        ops_test, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint
    )
    assert await is_entire_cluster_responding_requests(ops_test=ops_test, test_pod=test_pod)


@pytest.mark.abort_on_fail
async def test_invalid_service_type(
    ops_test,
):
    """Test the status of managed K8s service when `expose-external` is set to invalid value."""
    with pytest.raises(JujuUnitError):
        logger.info("Changing expose-external to an invalid value for kyuubi-k8s charm...")
        await ops_test.model.applications[APP_NAME].set_config({"expose-external": "invalid"})

        logger.info("Waiting for kyuubi-k8s app to be idle...")
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            timeout=1000,
        )
