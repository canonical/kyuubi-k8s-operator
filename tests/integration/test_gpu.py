#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.


import json
import logging
from pathlib import Path
from typing import cast

import jubilant
import yaml
from spark_test.utils import LightKubePod, get_spark_drivers, get_spark_executors

from core.domain import Status

from .helpers import (
    deploy_minimal_kyuubi_setup,
    fetch_connection_info,
    validate_sql_queries_with_kyuubi,
)
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def test_deploy_kyuubi_setup(
    juju: jubilant.Juju,
    kyuubi_charm: Path,
    charm_versions: IntegrationTestsCharms,
    s3_bucket_and_creds: S3Info,
) -> None:
    """Deploy the minimal setup for Kyuubi and assert all charms are in active and idle state."""
    deploy_minimal_kyuubi_setup(
        juju=juju,
        kyuubi_charm=kyuubi_charm,
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=True,
        integrate_zookeeper=True,
        integrate_data_integrator=True,
    )
    # Wait for everything to settle down
    juju.wait(jubilant.all_active, delay=5)

    logger.info("Deploying postgresql-k8s charm for metastore...")
    juju.deploy(**charm_versions.metastore_db.deploy_dict())

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    juju.wait(jubilant.all_active, delay=15, timeout=1000)

    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm...")
    juju.integrate(charm_versions.metastore_db.app, f"{APP_NAME}:metastore-db")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    juju.wait(jubilant.all_active, delay=20, timeout=1000)

    logger.info("Enabling GPU support")
    juju.config(APP_NAME, {"gpu-enable": True, "gpu-engine-executors-limit": 1})
    juju.wait(jubilant.all_active, delay=30, timeout=1000)


def test_gpu_used_for_query(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    """Check that the GPU resource is used by the executor pod, but not by the driver."""
    _, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    assert validate_sql_queries_with_kyuubi(juju, username=username, password=password)

    # The test plan should have spawn one driver and one executor
    executor_pods = get_spark_executors(namespace=cast(str, juju.model))
    driver_pods = get_spark_drivers(namespace=cast(str, juju.model))
    assert len(executor_pods) == 1
    assert len(driver_pods) == 1

    # The executor pod should request a GPU resource, and the RAPIDS plugin needs to be present
    # in its logs.
    exec_pod = executor_pods[0]
    pod_obj = exec_pod.client.get(
        LightKubePod, name=exec_pod.pod_name, namespace=exec_pod.namespace
    ).to_dict()
    assert "nvidia.com/gpu" in json.dumps(pod_obj)
    assert (
        "ExecutorPluginContainer: Initialized executor component for plugin com.nvidia.spark.SQLPlugin"
        in "".join(exec_pod.logs())
    )

    # The driver pod should not have requested a GPU resource
    driver_pod = driver_pods[0]
    pod_obj = driver_pod.client.get(
        LightKubePod, name=driver_pod.pod_name, namespace=driver_pod.namespace
    ).to_dict()
    assert "nvidia.com/gpu" not in json.dumps(pod_obj)


def test_request_more_gpu_than_capacity(juju: jubilant.Juju) -> None:
    juju.config(APP_NAME, {"gpu-engine-executors-limit": 99})
    status = juju.wait(lambda status: jubilant.all_blocked(status, APP_NAME), delay=5, timeout=500)
    assert status.apps[APP_NAME].app_status.message == Status.NOT_ENOUGH_GPUS.value.message
