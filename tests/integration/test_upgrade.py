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
    deploy_minimal_kyuubi_setup,
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
async def test_build_and_deploy(ops_test: OpsTest, charm_versions, s3_bucket_and_creds, test_pod):
    """Test building and deploying the charm without relation with any other charm."""
    await deploy_minimal_kyuubi_setup(
        ops_test=ops_test,
        kyuubi_charm="kyuubi-k8s",
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=True,
        num_units=3,
        integrate_zookeeper=True,
        deploy_from_charmhub=True,
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

    active_servers = await get_active_kyuubi_servers_list(
        ops_test, zookeeper_name=charm_versions.zookeeper.application_name
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


@pytest.mark.abort_on_fail
async def test_kyuubi_upgrades(ops_test: OpsTest, kyuubi_charm, test_pod, charm_versions):
    """Test the correct upgrade of a Kyuubi cluster."""
    # Retrieve the image to use from metadata.yaml
    image_version = METADATA["resources"]["kyuubi-image"]["upstream-source"]
    logger.info(f"Image version: {image_version}")

    leader_unit = None
    for unit in ops_test.model.applications[APP_NAME].units:
        if await unit.is_leader_from_status():
            leader_unit = unit
    assert leader_unit

    # TODO trigger pre-upgrade checks after the release of the first charm with the upgrade feature available.

    # test upgrade procedure
    logger.info("Upgrading Kyuubi...")

    # start refresh by upgrading to the current version
    await ops_test.model.applications[APP_NAME].refresh(
        path=kyuubi_charm,
        resources={"kyuubi-image": image_version},
    )

    async with ops_test.fast_forward(fast_interval="20s"):
        await asyncio.sleep(90)

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], timeout=1000, idle_period=180, raise_on_error=False
    )
    logger.info("Resume upgrade...")
    action = await leader_unit.run_action("resume-upgrade")
    await action.wait()
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], timeout=1000, idle_period=30, status="active"
    )

    # test that upgraded Kyuubi cluster works and all units are available
    assert await run_sql_test_against_jdbc_endpoint(ops_test, test_pod)

    active_servers = await get_active_kyuubi_servers_list(
        ops_test, zookeeper_name=charm_versions.zookeeper.application_name
    )
    assert len(active_servers) == 3

    expected_servers = [
        f"kyuubi-k8s-0.kyuubi-k8s-endpoints.{ops_test.model_name}.svc.cluster.local",
        f"kyuubi-k8s-1.kyuubi-k8s-endpoints.{ops_test.model_name}.svc.cluster.local",
        f"kyuubi-k8s-2.kyuubi-k8s-endpoints.{ops_test.model_name}.svc.cluster.local",
    ]
    assert set(active_servers) == set(expected_servers)

    logger.info("End of the tests.")
