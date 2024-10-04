import asyncio
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
    get_active_kyuubi_servers_list,
    run_sql_test_against_jdbc_endpoint,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
TEST_CHARM_PATH = "./tests/integration/app-charm"
TEST_CHARM_NAME = "application"
COS_AGENT_APP_NAME = "grafana-agent-k8s"


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


@pytest.mark.skip_if_deployed
@pytest.mark.abort_on_fail
async def test_build_and_deploy(
    ops_test: OpsTest, charm_versions, kyuubi_charm, s3_bucket_and_creds, test_pod
):
    """Test building and deploying the charm without relation with any other charm."""
    image_version = METADATA["resources"]["kyuubi-image"]["upstream-source"]
    # resources = {"kyuubi-image": image_version}
    logger.info(f"Image version: {image_version}")

    # Deploy the charm and wait for waiting status
    logger.info("Deploying kyuubi-k8s charm...")
    await ops_test.model.deploy(
        "kyuubi-k8s",
        application_name=APP_NAME,
        num_units=1,
        channel="edge",
        series="jammy",
        trust=True,
    )

    await ops_test.model.deploy(**charm_versions.zookeeper.deploy_dict())

    logger.info("Waiting for kyuubi-k8s app to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.zookeeper.application_name],
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
            "path": "testpath",
            "endpoint": endpoint_url,
        }
    )

    logger.info("Integrating kyuubi charm with s3-integrator charm...")
    await ops_test.model.integrate(charm_versions.s3.application_name, APP_NAME)

    logger.info("Waiting for s3-integrator and kyuubi charms to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.s3.application_name], timeout=1000
    )

    # Assert that both kyuubi-k8s and s3-integrator charms are in active state
    assert check_status(
        ops_test.model.applications[APP_NAME], Status.MISSING_INTEGRATION_HUB.value
    )

    assert ops_test.model.applications[charm_versions.s3.application_name].status == "active"

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

    logger.info("Integrating kyuubi charm with integration-hub charm...")
    await ops_test.model.integrate(charm_versions.integration_hub.application_name, APP_NAME)

    logger.info("Waiting for integration_hub and kyuubi charms to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.integration_hub.application_name],
        timeout=1000,
        status="active",
        idle_period=20,
    )

    # Assert that both kyuubi-k8s and s3-integrator charms are in active state
    assert check_status(ops_test.model.applications[APP_NAME], Status.ACTIVE.value)

    assert (
        ops_test.model.applications[charm_versions.integration_hub.application_name].status
        == "active"
    )

    # Scale Kyuubi charm to 3 units
    await ops_test.model.applications[APP_NAME].scale(scale=3)
    await ops_test.model.block_until(lambda: len(ops_test.model.applications[APP_NAME].units) == 3)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.zookeeper.application_name],
        timeout=1000,
        idle_period=40,
    )

    assert len(ops_test.model.applications[APP_NAME].units) == 3
    assert check_status(ops_test.model.applications[APP_NAME], Status.MISSING_ZOOKEEPER.value)

    # Integrate Kyuubi and Zookeeper
    logger.info("Integrating kyuubi charm with zookeeper charm...")
    await ops_test.model.integrate(charm_versions.zookeeper.application_name, APP_NAME)

    logger.info("Waiting for zookeeper-k8s and kyuubi charms to be active and idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.s3.application_name], timeout=1000, status="active"
    )

    # Assert that all charms is in active and idle state
    assert check_status(ops_test.model.applications[APP_NAME], Status.ACTIVE.value)
    assert ops_test.model.applications[charm_versions.s3.application_name].status == "active"
    assert (
        ops_test.model.applications[charm_versions.integration_hub.application_name].status
        == "active"
    )
    assert (
        ops_test.model.applications[charm_versions.zookeeper.application_name].status == "active"
    )

    active_servers = await get_active_kyuubi_servers_list(ops_test)
    assert len(active_servers) == 3

    expected_servers = [
        f"kyuubi-k8s-0.kyuubi-k8s-endpoints.{ops_test.model_name}.svc.cluster.local",
        f"kyuubi-k8s-1.kyuubi-k8s-endpoints.{ops_test.model_name}.svc.cluster.local",
        f"kyuubi-k8s-2.kyuubi-k8s-endpoints.{ops_test.model_name}.svc.cluster.local",
    ]
    assert set(active_servers) == set(expected_servers)

    # Run SQL test against the cluster
    assert await run_sql_test_against_jdbc_endpoint(ops_test, test_pod)

    leader_unit = None
    for unit in ops_test.model.applications[APP_NAME].units:
        if await unit.is_leader_from_status():
            leader_unit = unit
    assert leader_unit

    # ops_test.juju("")

    logger.info("Calling pre-upgrade-check...")
    # action = await leader_unit.run_action("pre-upgrade-check")
    # await action.wait()
    # await ops_test.model.wait_for_idle(
    #     apps=[APP_NAME], timeout=1000, idle_period=15, status="active"
    # )

    # test upgrade
    logger.info("Upgrading Kyuubi...")
    await ops_test.model.applications[APP_NAME].refresh(
        path=kyuubi_charm,
        resources={"kyuubi-image": image_version},
    )

    async with ops_test.fast_forward(fast_interval="20s"):
        await asyncio.sleep(90)

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], timeout=1000, idle_period=180, raise_on_error=False
    )
    ops_test.juju("status")
    logger.info("Resume upgrade...")
    action = await leader_unit.run_action("resume-upgrade")
    await action.wait()
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], timeout=1000, idle_period=30, status="active"
    )

    assert await run_sql_test_against_jdbc_endpoint(ops_test, test_pod)

    # logger.info("Checking that produced messages can be consumed afterwards...")
    # action = await ops_test.model.units.get(f"{DUMMY_NAME}/0").run_action("consume")
    # await action.wait()
    # await ops_test.model.wait_for_idle(
    #     apps=[APP_NAME, DUMMY_NAME], timeout=1000, idle_period=30, status="active"
    # )
