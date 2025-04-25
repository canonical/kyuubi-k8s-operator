#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path
from typing import cast

import yaml

from core.domain import Status

from .helpers import (
    fetch_spark_properties,
    jubilant,
    validate_sql_queries_with_kyuubi,
)
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def test_build_and_deploy_kyuubi(juju: jubilant.Juju, kyuubi_charm: Path) -> None:
    """Test building and deploying the charm without relation with any other charm."""
    image_version = METADATA["resources"]["kyuubi-image"]["upstream-source"]
    resources = {"kyuubi-image": image_version}
    logger.info(f"Image version: {image_version}")

    # Deploy the charm and wait for waiting status
    logger.info("Deploying kyuubi-k8s charm...")
    juju.deploy(
        kyuubi_charm,
        resources=resources,
        app=APP_NAME,
        num_units=1,
        base="ubuntu@22.04",
        trust=True,
    )

    logger.info("Setting configuration for kyuubi-k8s charm...")
    namespace = cast(str, juju.model)
    username = "kyuubi-spark-engine"
    juju.config(APP_NAME, {"namespace": namespace, "service-account": username})

    logger.info("Waiting for kyuubi-k8s app to be idle...")
    status = juju.wait(jubilant.all_blocked, delay=5)

    # Assert that the charm is in blocked state, waiting for Integration Hub relation
    assert status.apps[APP_NAME].app_status.message == Status.MISSING_INTEGRATION_HUB.value.message


def test_deploy_s3_integrator(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, s3_bucket_and_creds: S3Info
) -> None:
    """Test deploying the s3-integrator charm and configuring it."""
    # Deploy the charm and wait for waiting status
    logger.info("Deploying s3-integrator charm...")
    juju.deploy(**charm_versions.s3.deploy_dict())

    # Receive S3 params from fixture
    endpoint_url = s3_bucket_and_creds["endpoint"]
    access_key = s3_bucket_and_creds["access_key"]
    secret_key = s3_bucket_and_creds["secret_key"]
    bucket_name = s3_bucket_and_creds["bucket"]
    path = s3_bucket_and_creds["path"]

    juju.config(
        charm_versions.s3.app,
        {
            "bucket": bucket_name,
            "path": path,
            "endpoint": endpoint_url,
        },
    )
    juju.wait(jubilant.all_blocked)

    logger.info("Setting up s3 credentials in s3-integrator charm")
    task = juju.run(
        f"{charm_versions.s3.app}/0",
        "sync-s3-credentials",
        {"access-key": access_key, "secret-key": secret_key},
    )
    assert task.return_code == 0

    logger.info("Waiting for s3-integrator app to be idle and active...")

    juju.wait(lambda status: jubilant.all_active(status, charm_versions.s3.app))


def test_deploy_integration_hub(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test deploying the integration hub charm and configuring it."""
    # Deploy the charm and wait for waiting status
    logger.info("Deploying integration-hub charm...")
    juju.deploy(**charm_versions.integration_hub.deploy_dict())

    logger.info("Waiting for integration_hub app to be idle and active...")
    juju.wait(
        lambda status: jubilant.all_active(
            status, charm_versions.s3.app, charm_versions.integration_hub.app
        )
    )

    # Add configuration key
    task = juju.run(
        f"{charm_versions.integration_hub.app}/0",
        "add-config",
        {"conf": "spark.kubernetes.executor.request.cores=0.1"},
    )
    assert task.return_code == 0

    logger.info("Integrating s3-integrator charm with integration-hub charm...")
    juju.integrate(
        charm_versions.integration_hub.application_name, charm_versions.s3.application_name
    )

    logger.info("Waiting for integration_hub and s3-integrator charms to be idle and active...")
    juju.wait(
        lambda status: jubilant.all_active(
            status, charm_versions.s3.app, charm_versions.integration_hub.app
        ),
        delay=5,
    )


def test_integration_with_integration_hub(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test the integration with integration hub."""
    logger.info("Integrating kyuubi charm with integration-hub charm...")
    juju.integrate(charm_versions.integration_hub.application_name, APP_NAME)

    logger.info("Waiting for integration_hub and kyuubi charms to be idle and active...")
    juju.wait(jubilant.all_active, delay=5)


def test_integration_hub_realtime_updates(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test if the updates in integration hub are reflected in real-time in Kyuubi app."""
    logger.info("Removing relation between s3-integrator and integration-hub charm...")
    juju.remove_relation(
        f"{charm_versions.s3.application_name}:s3-credentials",
        f"{charm_versions.integration_hub.application_name}:s3-credentials",
    )
    logger.info("Waiting for integration_hub and s3-integrator charms to be idle and active...")
    juju.wait(
        lambda status: jubilant.all_active(
            status, charm_versions.s3.app, charm_versions.integration_hub.app
        ),
        delay=5,
    )

    logger.info("Waiting for kyuubi-k8s app to be idle...")
    status = juju.wait(
        lambda status: jubilant.all_blocked(status, APP_NAME),
    )

    # Assert that the charm is in blocked state, waiting for object storage backend
    assert (
        status.apps[APP_NAME].app_status.message
        == Status.MISSING_OBJECT_STORAGE_BACKEND.value.message
    )

    logger.info("Integrating s3-integrator charm again with integration-hub charm...")
    juju.integrate(
        charm_versions.integration_hub.application_name, charm_versions.s3.application_name
    )

    logger.info(
        "Waiting for integration_hub, kyuubi and s3-integrator charms to be idle and active..."
    )
    juju.wait(jubilant.all_active, delay=5)

    # Add a property via integration hub
    task = juju.run(
        f"{charm_versions.integration_hub.app}/0",
        "add-config",
        {"conf": "spark.kubernetes.executor.request.cores=0.1"},
    )
    assert task.return_code == 0

    logger.info(
        "Waiting for kyuubi, integration_hub and s3-integrator charms to be idle and active..."
    )
    juju.wait(jubilant.all_active, delay=5)

    props = fetch_spark_properties(juju, unit_name=f"{APP_NAME}/0")
    assert "foo" in props
    assert props["foo"] == "bar"

    # Remove the property via integration hub
    task = juju.run(
        f"{charm_versions.integration_hub.app}/0",
        "remove-config",
        {"key": "foo"},
    )
    assert task.return_code == 0

    logger.info(
        "Waiting for kyuubi, integration_hub and s3-integrator charms to be idle and active..."
    )
    juju.wait(jubilant.all_active, delay=5)

    props = fetch_spark_properties(juju, unit_name=f"{APP_NAME}/0")
    assert "foo" not in props


# TODO: Revisit this test after recent updates in the purpose of Kyuubi <> Zookeeper relation
def test_integration_with_zookeeper(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test the charm by integrating it with Zookeeper."""
    # Deploy the charm and wait for waiting status
    logger.info("Deploying zookeeper-k8s charm...")
    juju.deploy(**charm_versions.zookeeper.deploy_dict())

    logger.info("Waiting for zookeeper app to be active and idle...")
    juju.wait(jubilant.all_active, delay=5)

    logger.info("Integrating kyuubi charm with zookeeper charm...")
    juju.integrate(charm_versions.zookeeper.application_name, APP_NAME)

    logger.info("Waiting for zookeeper-k8s and kyuubi charms to be idle idle...")
    juju.wait(jubilant.all_active, delay=5)

    assert validate_sql_queries_with_kyuubi(juju)


# TODO: Revisit this test after recent updates in the purpose of Kyuubi <> Zookeeper relation
def test_remove_zookeeper_relation(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test the charm after the zookeeper relation has been broken."""
    logger.info("Removing relation between zookeeper-k8s and kyuubi-k8s...")
    juju.remove_relation(f"{APP_NAME}:zookeeper", f"{charm_versions.zookeeper.app}:zookeeper")

    logger.info("Waiting for zookeeper-k8s and kyuubi-k8s apps to be idle and active...")
    juju.wait(jubilant.all_active, delay=5)

    assert validate_sql_queries_with_kyuubi(juju)


# TODO: rewrite
# @pytest.mark.skip(reason="This tests need re-write and fixes on integration hub level")
# def test_read_spark_properties_from_secrets(ops_test: OpsTest):
#     """Test that the spark properties provided via K8s secrets (spark8t library) are picked by Kyuubi."""
#     namespace = ops_test.model.name
#     sa_name = "custom-sa"

#     # Adding a custom property via Spark8t to the service account
#     assert (
#         subprocess.run(
#             [
#                 "python",
#                 "-m",
#                 "spark8t.cli.service_account_registry",
#                 "create",
#                 "--username",
#                 sa_name,
#                 "--namespace",
#                 namespace,
#                 "--conf",
#                 "spark.kubernetes.executor.request.cores=0.1",
#                 "--conf",
#                 "spark.executor.instances=3",
#             ]
#         ).returncode
#         == 0
#     )

#     logger.info("Changing configuration for kyuubi-k8s charm...")
#     await ops_test.model.applications[APP_NAME].set_config({"service-account": sa_name})

#     logger.info("Waiting for kyuubi-k8s app to be idle...")
#     await ops_test.model.wait_for_idle(
#         apps=[APP_NAME],
#         status="active",
#         timeout=1000,
#     )

#     assert await validate_sql_queries_with_kyuubi(ops_test=ops_test)

#     # Check exactly 3 executor pods were created.
#     list_pods_process = subprocess.run(
#         ["kubectl", "get", "pods", "-n", namespace, "--sort-by", ".metadata.creationTimestamp"],
#         capture_output=True,
#     )

#     assert list_pods_process.returncode == 0

#     pods_list = list_pods_process.stdout.decode().splitlines()

#     driver_pod_name = ""
#     executor_pod_names = []

#     # Last 4 pods in the list are of interest,
#     # one is the driver and 3 should be executor pods
#     for pod in pods_list[-4:]:
#         name = pod.split()[0]
#         if "driver" in name:
#             driver_pod_name = name
#         else:
#             executor_pod_names.append(name)

#     expected_executor_pod_names = [
#         driver_pod_name.replace("driver", "exec-1"),
#         driver_pod_name.replace("driver", "exec-2"),
#         driver_pod_name.replace("driver", "exec-3"),
#     ]

#     assert len(executor_pod_names) == len(expected_executor_pod_names)
#     assert set(executor_pod_names) == set(expected_executor_pod_names)
