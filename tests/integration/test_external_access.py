#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import json
import logging
from pathlib import Path
from typing import cast

import jubilant
import yaml

from .helpers import (
    assert_service_status,
    deploy_minimal_kyuubi_setup,
    fetch_connection_info,
    is_entire_cluster_responding_requests,
    run_sql_test_against_jdbc_endpoint,
)
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def test_default_deploy(
    juju: jubilant.Juju,
    kyuubi_charm: Path,
    charm_versions: IntegrationTestsCharms,
    s3_bucket_and_creds: S3Info,
    test_pod: str,
) -> None:
    """Test the status of default managed K8s service when Kyuubi is deployed."""
    deploy_minimal_kyuubi_setup(
        juju=juju,
        kyuubi_charm=kyuubi_charm,
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=True,
        num_units=3,
        integrate_zookeeper=True,
    )

    # Wait for everything to settle down
    juju.wait(jubilant.all_active, delay=15)

    # Ensure that Kyuubi is exposed with ClusterIP service
    assert_service_status(namespace=cast(str, juju.model), service_type="ClusterIP")

    jdbc_endpoint, username, password = fetch_connection_info(
        juju, charm_versions.data_integrator.app
    )

    # Run SQL tests against JDBC endpoint
    assert run_sql_test_against_jdbc_endpoint(
        juju, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint, username=username, password=password
    )
    assert is_entire_cluster_responding_requests(
        juju, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint, username=username, password=password
    )


def test_nodeport_service(
    juju: jubilant.Juju, test_pod: str, charm_versions: IntegrationTestsCharms
) -> None:
    """Test the status of managed K8s service when `expose-external` is set to 'nodeport'."""
    logger.info("Changing expose-external to 'nodeport' for kyuubi-k8s charm...")
    juju.config(APP_NAME, {"expose-external": "nodeport"})

    logger.info("Waiting for kyuubi-k8s app to be active and idle...")
    juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME),
        delay=5,
    )

    assert_service_status(namespace=cast(str, juju.model), service_type="NodePort")

    jdbc_endpoint, username, password = fetch_connection_info(
        juju, charm_versions.data_integrator.app
    )

    # Run SQL tests against JDBC endpoint
    assert run_sql_test_against_jdbc_endpoint(
        juju, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint, username=username, password=password
    )
    assert is_entire_cluster_responding_requests(
        juju, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint, username=username, password=password
    )


def test_loadbalancer_service(
    juju: jubilant.Juju, test_pod: str, charm_versions: IntegrationTestsCharms
) -> None:
    """Test the status of managed K8s service when `expose-external` is set to 'loadbalancer'."""
    logger.info("Changing expose-external to 'nodeport' for kyuubi-k8s charm...")
    juju.config(
        APP_NAME,
        {
            "expose-external": "loadbalancer",
            "loadbalancer-extra-annotations": json.dumps({"foo": "bar"}),
        },
    )

    logger.info("Waiting for kyuubi-k8s app to be active and idle...")
    juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME),
        delay=5,
    )

    service = assert_service_status(namespace=cast(str, juju.model), service_type="LoadBalancer")
    annotations = getattr(service.metadata, "annotations", {})
    assert annotations.get("foo", "") == "bar"

    jdbc_endpoint, username, password = fetch_connection_info(
        juju, charm_versions.data_integrator.app
    )

    # Run SQL tests against JDBC endpoint
    assert run_sql_test_against_jdbc_endpoint(
        juju, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint, username=username, password=password
    )
    assert is_entire_cluster_responding_requests(
        juju, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint, username=username, password=password
    )


def test_clusterip_service(
    juju: jubilant.Juju, test_pod: str, charm_versions: IntegrationTestsCharms
) -> None:
    """Test the status of managed K8s service when `expose-external` is set to 'false'."""
    logger.info("Changing expose-external to 'false' for kyuubi-k8s charm...")
    juju.config(APP_NAME, {"expose-external": "false"})

    logger.info("Waiting for kyuubi-k8s app to be active and idle...")
    juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME),
        delay=5,
    )
    assert_service_status(namespace=cast(str, juju.model), service_type="ClusterIP")

    jdbc_endpoint, username, password = fetch_connection_info(
        juju, charm_versions.data_integrator.app
    )

    # Run SQL tests against JDBC endpoint
    assert run_sql_test_against_jdbc_endpoint(
        juju, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint, username=username, password=password
    )
    assert is_entire_cluster_responding_requests(
        juju, test_pod=test_pod, jdbc_endpoint=jdbc_endpoint, username=username, password=password
    )


def test_invalid_service_type(
    juju: jubilant.Juju,
):
    """Test the status of managed K8s service when `expose-external` is set to invalid value."""
    logger.info("Changing expose-external to an invalid value for kyuubi-k8s charm...")
    juju.config(APP_NAME, {"expose-external": "invalid"})

    juju.wait(
        lambda status: {
            status.apps[APP_NAME].units[unit].workload_status.current
            for unit in status.apps[APP_NAME].units
        }
        == {"error"}
    )
