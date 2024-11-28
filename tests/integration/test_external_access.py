#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import pytest
import yaml

from core.domain import Status

from .helpers import (
    check_status,
    deploy_minimal_kyuubi_setup,
    fetch_jdbc_endpoint,
    get_k8s_service,
    run_sql_test_against_jdbc_endpoint,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
TEST_CHARM_PATH = "./tests/integration/app-charm"
TEST_CHARM_NAME = "application"
JDBC_PORT = 10009
JDBC_PORT_NAME = "kyuubi-jdbc"
NODEPORT_MIN_VALUE = 30000
NODEPORT_MAX_VALUE = 32767


def assert_service_status(
    namespace,
    service_type,
):
    """Utility function to check status of managed K8s service created by Kyuubi charm."""
    service_name = f"{APP_NAME}-service"
    service = get_k8s_service(namespace=namespace, service_name=service_name)
    logger.info(f"{service=}")

    assert service is not None

    service_spec = service.spec
    assert service_type == service_spec.type
    assert service_spec.selector == {"app.kubernetes.io/name": APP_NAME}

    service_port = service_spec.ports[0]
    assert service_port.port == JDBC_PORT
    assert service_port.targetPort == JDBC_PORT
    assert service_port.name == JDBC_PORT_NAME
    assert service_port.protocol == "TCP"

    if service_type in ("NodePort", "LoadBalancer"):
        assert NODEPORT_MIN_VALUE <= service_port.nodePort <= NODEPORT_MAX_VALUE


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
    )

    assert_service_status(namespace=ops_test.model_name, service_type="ClusterIP")

    jdbc_endpoint = await fetch_jdbc_endpoint(ops_test)
    assert await run_sql_test_against_jdbc_endpoint(
        ops_test, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint
    )


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


@pytest.mark.abort_on_fail
async def test_invalid_service_type(
    ops_test,
):
    """Test the status of managed K8s service when `expose-external` is set to invalid value."""
    logger.info("Changing expose-external to an invalid value for kyuubi-k8s charm...")
    await ops_test.model.applications[APP_NAME].set_config({"expose-external": "invalid"})

    logger.info("Waiting for kyuubi-k8s app to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        timeout=1000,
    )

    # Assert that the charm is in blocked state, due to invalid value of expose-external
    assert check_status(
        ops_test.model.applications[APP_NAME], Status.INVALID_EXPOSE_EXTERNAL.value
    )
