#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
import subprocess
from pathlib import Path

import psycopg2
import pytest
import yaml
from pytest_operator.plugin import OpsTest
from thrift.transport.TTransport import TTransportException

from constants import AUTHENTICATION_DATABASE_NAME
from core.domain import Status

from .helpers import (
    check_status,
    fetch_spark_properties,
    find_leader_unit,
    get_address,
    validate_sql_queries_with_kyuubi,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


@pytest.mark.skip_if_deployed
@pytest.mark.abort_on_fail
async def test_build_and_deploy_kyuubi(ops_test: OpsTest, kyuubi_charm: Path) -> None:
    """Test building and deploying the charm without relation with any other charm."""
    image_version = METADATA["resources"]["kyuubi-image"]["upstream-source"]
    resources = {"kyuubi-image": image_version}
    logger.info(f"Image version: {image_version}")

    # Deploy the charm and wait for waiting status
    logger.info("Deploying kyuubi-k8s charm...")
    await ops_test.model.deploy(
        kyuubi_charm,
        resources=resources,
        application_name=APP_NAME,
        num_units=1,
        series="jammy",
        trust=True,
    )

    logger.info("Waiting for kyuubi-k8s app to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        timeout=1000,
    )
    logger.info(f"State of kyuubi-k8s app: {ops_test.model.applications[APP_NAME].status}")

    logger.info("Setting configuration for kyuubi-k8s charm...")
    namespace = ops_test.model.name
    username = "kyuubi-spark-engine"
    await ops_test.model.applications[APP_NAME].set_config(
        {"namespace": namespace, "service-account": username}
    )

    logger.info("Waiting for kyuubi-k8s app to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="blocked",
        timeout=1000,
    )

    # Assert that the charm is in blocked state, waiting for Integration Hub relation
    assert check_status(
        ops_test.model.applications[APP_NAME], Status.MISSING_INTEGRATION_HUB.value
    )


@pytest.mark.abort_on_fail
async def test_deploy_s3_integrator(ops_test: OpsTest, charm_versions, s3_bucket_and_creds):
    """Test deploying the s3-integrator charm and configuring it."""
    # Deploy the charm and wait for waiting status
    logger.info("Deploying s3-integrator charm...")
    await ops_test.model.deploy(**charm_versions.s3.deploy_dict())

    logger.info("Waiting for s3-integrator app to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.s3.application_name], timeout=1000
    )

    # Receive S3 params from fixture
    endpoint_url = s3_bucket_and_creds["endpoint"]
    access_key = s3_bucket_and_creds["access_key"]
    secret_key = s3_bucket_and_creds["secret_key"]
    bucket_name = s3_bucket_and_creds["bucket"]
    path = s3_bucket_and_creds["path"]

    logger.info("Setting up s3 credentials in s3-integrator charm")
    s3_integrator_unit = ops_test.model.applications[charm_versions.s3.application_name].units[0]
    action = await s3_integrator_unit.run_action(
        action_name="sync-s3-credentials", **{"access-key": access_key, "secret-key": secret_key}
    )
    await action.wait()

    logger.info("Waiting for s3-integrator app to be idle and active...")
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[charm_versions.s3.application_name], status="active"
        )

    logger.info("Setting configuration for s3-integrator charm...")
    await ops_test.model.applications[charm_versions.s3.application_name].set_config(
        {
            "bucket": bucket_name,
            "path": path,
            "endpoint": endpoint_url,
        }
    )

    logger.info("Waiting for s3-integrator charm to be idle...")
    await ops_test.model.wait_for_idle(apps=[charm_versions.s3.application_name], timeout=1000)
    assert ops_test.model.applications[charm_versions.s3.application_name].status == "active"


@pytest.mark.abort_on_fail
async def test_deploy_integration_hub(ops_test: OpsTest, charm_versions):
    """Test deploying the integration hub charm and configuring it."""
    # Deploy the charm and wait for waiting status
    logger.info("Deploying integration-hub charm...")
    await ops_test.model.deploy(**charm_versions.integration_hub.deploy_dict())

    logger.info("Waiting for integration_hub app to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[charm_versions.integration_hub.application_name], timeout=1000, status="active"
    )

    # Add configuration key
    unit = ops_test.model.applications[charm_versions.integration_hub.application_name].units[0]
    action = await unit.run_action(
        action_name="add-config", conf="spark.kubernetes.executor.request.cores=0.1"
    )
    _ = await action.wait()

    logger.info("Integrating s3-integrator charm with integration-hub charm...")
    await ops_test.model.integrate(
        charm_versions.integration_hub.application_name, charm_versions.s3.application_name
    )

    logger.info("Waiting for integration_hub and s3-integrator charms to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[charm_versions.s3.application_name, charm_versions.integration_hub.application_name],
        timeout=1000,
        status="active",
        idle_period=20,
    )

    # Assert that both integration-hub and s3-integrator charms are in active state
    assert (
        ops_test.model.applications[charm_versions.integration_hub.application_name].status
        == "active"
    )
    assert ops_test.model.applications[charm_versions.s3.application_name].status == "active"


@pytest.mark.abort_on_fail
async def test_integration_with_integration_hub(ops_test: OpsTest, charm_versions):
    """Test the integration with integration hub."""
    logger.info("Integrating kyuubi charm with integration-hub charm...")
    await ops_test.model.integrate(charm_versions.integration_hub.application_name, APP_NAME)

    logger.info("Waiting for integration_hub and kyuubi charms to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.integration_hub.application_name],
        timeout=1000,
        idle_period=20,
    )


@pytest.mark.abort_on_fail
async def test_kyuubi_without_passing_credentials(ops_test: OpsTest):
    """Test running SQL queries when authentication has not yet been enabled."""
    with pytest.raises(TTransportException):
        assert await validate_sql_queries_with_kyuubi(ops_test=ops_test)


@pytest.mark.abort_on_fail
async def test_enable_authentication(ops_test, charm_versions):
    """Test the Kyuuubi charm by integrating it with external metastore."""
    # Deploy the charm and wait for waiting status
    logger.info("Deploying postgresql-k8s charm...")
    await ops_test.model.deploy(**charm_versions.postgres.deploy_dict())

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[charm_versions.postgres.application_name], timeout=1000, status="active"
    )

    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm...")
    await ops_test.model.integrate(charm_versions.postgres.application_name, f"{APP_NAME}:auth-db")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.postgres.application_name], timeout=1000
    )

    # Assert that both kyuubi-k8s and postgresql-k8s charms are in active state
    assert check_status(ops_test.model.applications[APP_NAME], Status.ACTIVE.value)
    assert ops_test.model.applications[charm_versions.postgres.application_name].status == "active"

    postgres_leader = await find_leader_unit(
        ops_test, app_name=charm_versions.postgres.application_name
    )
    assert postgres_leader is not None
    postgres_host = await get_address(ops_test, unit_name=postgres_leader.name)

    action = await postgres_leader.run_action(
        action_name="get-password",
    )
    result = await action.wait()
    password = result.results.get("password")

    # Connect to PostgreSQL metastore database
    connection = psycopg2.connect(
        host=postgres_host,
        database=AUTHENTICATION_DATABASE_NAME,
        user="operator",
        password=password,
    )

    with connection.cursor() as cursor:
        cursor.execute(""" SELECT * FROM kyuubi_users; """)
        assert cursor.rowcount != 0

    connection.close()


@pytest.mark.abort_on_fail
async def test_integration_hub_realtime_updates(ops_test: OpsTest, charm_versions):
    """Test if the updates in integration hub are reflected in real-time in Kyuubi app."""
    logger.info("Removing relation between s3-integrator and integration-hub charm...")
    await ops_test.model.applications[f"{charm_versions.s3.application_name}"].remove_relation(
        f"{charm_versions.s3.application_name}:s3-credentials",
        f"{charm_versions.integration_hub.application_name}:s3-credentials",
    )
    logger.info("Waiting for integration_hub and s3-integrator charms to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[charm_versions.s3.application_name, charm_versions.integration_hub.application_name],
        timeout=1000,
        status="active",
        idle_period=20,
    )
    logger.info("Waiting for kyuubi-k8s app to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="blocked",
        timeout=1000,
    )
    # Assert that the charm is in blocked state, waiting for object storage backend
    assert check_status(
        ops_test.model.applications[APP_NAME], Status.MISSING_OBJECT_STORAGE_BACKEND.value
    )

    logger.info("Integrating s3-integrator charm again with integration-hub charm...")
    await ops_test.model.integrate(
        charm_versions.integration_hub.application_name, charm_versions.s3.application_name
    )

    logger.info(
        "Waiting for integration_hub, kyuubi and s3-integrator charms to be idle and active..."
    )
    await ops_test.model.wait_for_idle(
        apps=[
            charm_versions.s3.application_name,
            charm_versions.integration_hub.application_name,
            APP_NAME,
        ],
        timeout=1000,
        status="active",
        idle_period=20,
    )

    # Assert that all kyuubi-k8s, integration-hub and s3-integrator charms are in active state
    assert check_status(ops_test.model.applications[APP_NAME], Status.ACTIVE.value)
    assert (
        ops_test.model.applications[charm_versions.integration_hub.application_name].status
        == "active"
    )
    assert ops_test.model.applications[charm_versions.s3.application_name].status == "active"

    # Add a property via integration hub
    unit = ops_test.model.applications[charm_versions.integration_hub.application_name].units[0]
    action = await unit.run_action(action_name="add-config", conf="foo=bar")
    _ = await action.wait()

    logger.info(
        "Waiting for kyuubi, integration_hub and s3-integrator charms to be idle and active..."
    )
    await ops_test.model.wait_for_idle(
        apps=[
            APP_NAME,
            charm_versions.s3.application_name,
            charm_versions.integration_hub.application_name,
        ],
        timeout=1000,
        status="active",
        idle_period=20,
    )

    props = await fetch_spark_properties(ops_test, unit_name=f"{APP_NAME}/0")
    assert "foo" in props
    assert props["foo"] == "bar"

    # Remove the property via integration hub
    unit = ops_test.model.applications[charm_versions.integration_hub.application_name].units[0]
    action = await unit.run_action(action_name="remove-config", key="foo")
    _ = await action.wait()

    logger.info(
        "Waiting for kyuubi, integration_hub and s3-integrator charms to be idle and active..."
    )
    await ops_test.model.wait_for_idle(
        apps=[
            APP_NAME,
            charm_versions.s3.application_name,
            charm_versions.integration_hub.application_name,
        ],
        timeout=1000,
        status="active",
        idle_period=20,
    )

    props = await fetch_spark_properties(ops_test, unit_name=f"{APP_NAME}/0")
    assert "foo" not in props


# TODO: Revisit this test after recent updates in the purpose of Kyuubi <> Zookeeper relation
@pytest.mark.abort_on_fail
async def test_integration_with_zookeeper(ops_test: OpsTest, charm_versions):
    """Test the charm by integrating it with Zookeeper."""
    # Deploy the charm and wait for waiting status
    logger.info("Deploying zookeeper-k8s charm...")
    await ops_test.model.deploy(**charm_versions.zookeeper.deploy_dict())

    logger.info("Waiting for zookeeper app to be active and idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.zookeeper.application_name], timeout=1000, status="active"
    )

    logger.info("Integrating kyuubi charm with zookeeper charm...")
    await ops_test.model.integrate(charm_versions.zookeeper.application_name, APP_NAME)

    logger.info("Waiting for zookeeper-k8s and kyuubi charms to be idle idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.zookeeper.application_name], timeout=1000, status="active"
    )
    kyuubi_leader = await find_leader_unit(ops_test, app_name=APP_NAME)
    assert kyuubi_leader is not None

    logger.info("Running action 'get-password' on kyuubi-k8s unit...")
    action = await kyuubi_leader.run_action(
        action_name="get-password",
    )
    result = await action.wait()

    password = result.results.get("password")
    logger.info(f"Fetched password: {password}")

    username = "admin"
    assert await validate_sql_queries_with_kyuubi(
        ops_test=ops_test, username=username, password=password
    )


# TODO: Revisit this test after recent updates in the purpose of Kyuubi <> Zookeeper relation
@pytest.mark.abort_on_fail
async def test_remove_zookeeper_relation(ops_test: OpsTest, charm_versions):
    """Test the charm after the zookeeper relation has been broken."""
    logger.info("Removing relation between zookeeper-k8s and kyuubi-k8s...")
    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:zookeeper", f"{charm_versions.zookeeper.application_name}:zookeeper"
    )

    logger.info("Waiting for zookeeper-k8s and kyuubi-k8s apps to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.zookeeper.application_name], timeout=1000, status="active"
    )
    kyuubi_leader = await find_leader_unit(ops_test, app_name=APP_NAME)
    assert kyuubi_leader is not None

    logger.info("Running action 'get-password' on kyuubi-k8s unit...")
    action = await kyuubi_leader.run_action(
        action_name="get-password",
    )
    result = await action.wait()

    password = result.results.get("password")
    logger.info(f"Fetched password: {password}")

    username = "admin"

    assert await validate_sql_queries_with_kyuubi(
        ops_test=ops_test, username=username, password=password
    )


@pytest.mark.skip(reason="This tests need re-write and fixes on integration hub level")
@pytest.mark.abort_on_fail
async def test_read_spark_properties_from_secrets(ops_test: OpsTest):
    """Test that the spark properties provided via K8s secrets (spark8t library) are picked by Kyuubi."""
    namespace = ops_test.model.name
    sa_name = "custom-sa"

    # Adding a custom property via Spark8t to the service account
    assert (
        subprocess.run(
            [
                "python",
                "-m",
                "spark8t.cli.service_account_registry",
                "create",
                "--username",
                sa_name,
                "--namespace",
                namespace,
                "--conf",
                "spark.kubernetes.executor.request.cores=0.1",
                "--conf",
                "spark.executor.instances=3",
            ]
        ).returncode
        == 0
    )

    logger.info("Changing configuration for kyuubi-k8s charm...")
    await ops_test.model.applications[APP_NAME].set_config({"service-account": sa_name})

    logger.info("Waiting for kyuubi-k8s app to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )
    kyuubi_leader = await find_leader_unit(ops_test, app_name=APP_NAME)
    assert kyuubi_leader is not None

    logger.info("Running action 'get-password' on kyuubi-k8s unit...")
    action = await kyuubi_leader.run_action(
        action_name="get-password",
    )
    result = await action.wait()

    password = result.results.get("password")
    logger.info(f"Fetched password: {password}")

    username = "admin"
    assert await validate_sql_queries_with_kyuubi(
        ops_test=ops_test, username=username, password=password
    )

    # Check exactly 3 executor pods were created.
    list_pods_process = subprocess.run(
        ["kubectl", "get", "pods", "-n", namespace, "--sort-by", ".metadata.creationTimestamp"],
        capture_output=True,
    )

    assert list_pods_process.returncode == 0

    pods_list = list_pods_process.stdout.decode().splitlines()

    driver_pod_name = ""
    executor_pod_names = []

    # Last 4 pods in the list are of interest,
    # one is the driver and 3 should be executor pods
    for pod in pods_list[-4:]:
        name = pod.split()[0]
        if "driver" in name:
            driver_pod_name = name
        else:
            executor_pod_names.append(name)

    expected_executor_pod_names = [
        driver_pod_name.replace("driver", "exec-1"),
        driver_pod_name.replace("driver", "exec-2"),
        driver_pod_name.replace("driver", "exec-3"),
    ]

    assert len(executor_pod_names) == len(expected_executor_pod_names)
    assert set(executor_pod_names) == set(expected_executor_pod_names)
