#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import logging
import subprocess
from pathlib import Path

import pytest
import yaml
from juju.application import Application
from juju.unit import Unit
from ops import StatusBase
from pytest_operator.plugin import OpsTest

from core.domain import Status

from .helpers import get_address

logger = logging.getLogger(__name__)

TLS_NAME = "self-signed-certificates"

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
    ops_test: OpsTest, kyuubi_charm, charm_versions, s3_bucket_and_creds
):
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

    logger.info("Deploying s3-integrator charm...")
    await ops_test.model.deploy(**charm_versions.s3.deploy_dict()),

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
    await ops_test.model.deploy(**charm_versions.integration_hub.deploy_dict()),

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


@pytest.mark.abort_on_fail
async def test_jdbc_endpoint_with_default_metastore(ops_test: OpsTest, test_pod):
    """Test the JDBC endpoint exposed by the charm."""
    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    kyuubi_unit = ops_test.model.applications[APP_NAME].units[0]
    action = await kyuubi_unit.run_action(
        action_name="get-jdbc-endpoint",
    )
    result = await action.wait()

    jdbc_endpoint = result.results.get("endpoint")
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    logger.info(
        "Testing JDBC endpoint by connecting with beeline" " and executing a few SQL queries..."
    )
    process = subprocess.run(
        [
            "./tests/integration/test_jdbc_endpoint.sh",
            test_pod,
            ops_test.model_name,
            jdbc_endpoint,
            "db_default_metastore",
            "table_default_metastore",
        ],
        capture_output=True,
    )
    print("========== test_jdbc_endpoint.sh STDOUT =================")
    print(process.stdout.decode())
    print("========== test_jdbc_endpoint.sh STDERR =================")
    print(process.stderr.decode())
    logger.info(f"JDBC endpoint test returned with status {process.returncode}")
    assert process.returncode == 0


@pytest.mark.abort_on_fail
async def test_deploy_ssl(ops_test: OpsTest, charm_versions):
    await asyncio.gather(
        ops_test.model.deploy(
            charm_versions.tls.application_name,
            application_name=charm_versions.tls.application_name,
            channel=charm_versions.tls.channel,
            num_units=1,
            config={"ca-common-name": "kyuubi"},
            series=charm_versions.tls.series,
            # FIXME (certs): Unpin the revision once the charm is fixed
            revision=charm_versions.tls.revision,
        ),
    )
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, TLS_NAME], status="active", timeout=1000, idle_period=30
    )
    await ops_test.model.add_relation(APP_NAME, TLS_NAME)

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, TLS_NAME],
            status="active",
            timeout=1000,
            idle_period=30,
        )

    host = await get_address(ops_test, unit_name=f"{APP_NAME}/0")

    response = subprocess.check_output(
        f"openssl s_client -showcerts -connect {host}:10009 < /dev/null",
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True,
    )

    assert "TLSv1.3" in response
    assert "CN = kyuubi" in response


# @pytest.mark.abort_on_fail
# async def test_scale_up_tls(ops_test: OpsTest):
#     await ops_test.model.applications[APP_NAME].add_units(count=1)
#     await ops_test.model.block_until(lambda: len(ops_test.model.applications[APP_NAME].units) == 4)
#     await ops_test.model.wait_for_idle(
#         apps=[APP_NAME], status="active", timeout=1000, idle_period=40
#     )
#     check here


@pytest.mark.abort_on_fail
async def test_renew_cert(ops_test: OpsTest):
    # invalidate previous certs
    await ops_test.model.applications[TLS_NAME].set_config({"ca-common-name": "new-name"})

    await ops_test.model.wait_for_idle([APP_NAME], status="active", timeout=1000, idle_period=30)
    async with ops_test.fast_forward(fast_interval="20s"):
        await asyncio.sleep(60)

    # check client-presented certs
    host = await get_address(ops_test, unit_name=f"{APP_NAME}/0")

    response = subprocess.check_output(
        f"openssl s_client -showcerts -connect {host}:10009 < /dev/null",
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True,
    )

    assert "TLSv1.3" in response

    response = subprocess.check_output(
        f"openssl s_client -showcerts -connect {host}:10009 < /dev/null",
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True,
    )

    assert "CN = new-name" in response
