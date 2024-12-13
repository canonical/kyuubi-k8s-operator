#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import ast
import asyncio
import json
import logging
import os
import subprocess
from pathlib import Path

import pytest
import yaml
from juju.application import Application
from juju.unit import Unit
from ops import StatusBase
from pytest_operator.plugin import OpsTest

from core.domain import Status

from .helpers import get_address, run_command_in_pod, umask_named_temporary_file

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

TRUSTSTORE_PASSWORD = "password"
CERTIFICATE_LOCATION = "/tmp/cert.pem"
TRUSTSTORE_LOCATION = "/tmp/truststore.jks"


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
async def test_enable_ssl(ops_test: OpsTest, charm_versions, test_pod):
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
        apps=[APP_NAME, charm_versions.tls.name], status="active", timeout=1000, idle_period=30
    )
    await ops_test.model.add_relation(APP_NAME, charm_versions.tls.name)

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, charm_versions.tls.name],
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

    # get issued certificates
    logger.info("Get certificate from self-signed certificate operator")
    self_signed_certificate_unit = ops_test.model.applications[
        charm_versions.tls.application_name
    ].units[0]
    action = await self_signed_certificate_unit.run_action(
        action_name="get-issued-certificates",
    )
    result = await action.wait()
    items = ast.literal_eval(result.results.get("certificates"))
    certificates = json.loads(items[0])
    cert = certificates["certificate"]

    logger.info(f"Copy the certificate to the testpod in this location: {CERTIFICATE_LOCATION}")
    with umask_named_temporary_file(
        mode="w",
        prefix="cert-",
        suffix=".conf",
        dir=os.path.expanduser("~"),
    ) as temp_file:
        with open(temp_file.name, "w+") as f:
            f.writelines(cert)
        kubectl_command = [
            "kubectl",
            "cp",
            "-n",
            ops_test.model_name,
            temp_file.name,
            f"{test_pod}:{CERTIFICATE_LOCATION}",
            "-c",
            "kyuubi",
        ]
        process = subprocess.run(kubectl_command, capture_output=True, check=True)
        logger.info(process.stdout.decode())
        logger.info(process.stderr.decode())
        assert process.returncode == 0

        logger.info(
            f"Generating the trustore in the testpod in this location: {TRUSTSTORE_LOCATION}"
        )
        c2 = [
            "keytool",
            "-importcert",
            "--noprompt",
            "-alias",
            "mycert",
            "-file",
            CERTIFICATE_LOCATION,
            "-keystore",
            TRUSTSTORE_LOCATION,
            "-storepass",
            TRUSTSTORE_PASSWORD,
        ]

    await run_command_in_pod(ops_test, "testpod", c2)
    # mod permission of the trustore
    c3 = ["chmod", "u+x", TRUSTSTORE_LOCATION]
    await run_command_in_pod(ops_test, "testpod", c3)

    # run query with tls
    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    kyuubi_unit = ops_test.model.applications[APP_NAME].units[0]
    action = await kyuubi_unit.run_action(
        action_name="get-jdbc-endpoint",
    )
    result = await action.wait()

    jdbc_endpoint = result.results.get("endpoint")
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    jdbc_endpoint_ssl = (
        jdbc_endpoint
        + f";ssl=true;trustStorePassword={TRUSTSTORE_PASSWORD};sslTrustStore={TRUSTSTORE_LOCATION}"
    )
    logger.info(f"JDBC endpoint with SSL: {jdbc_endpoint_ssl}")

    logger.info(
        "Testing JDBC endpoint by connecting with beeline" " and executing a few SQL queries..."
    )
    process = subprocess.run(
        [
            "./tests/integration/test_jdbc_endpoint.sh",
            test_pod,
            ops_test.model_name,
            jdbc_endpoint_ssl,
            "db_default_metastore_1",
            "table_default_metastore_1",
        ],
        capture_output=True,
    )
    print("========== test_jdbc_endpoint.sh STDOUT =================")
    print(process.stdout.decode())
    print("========== test_jdbc_endpoint.sh STDERR =================")
    print(process.stderr.decode())
    logger.info(f"JDBC endpoint test returned with status {process.returncode}")
    assert process.returncode == 0
    logger.info("SSL connection established correctly.")


@pytest.mark.abort_on_fail
async def test_renew_cert(ops_test: OpsTest, charm_versions):
    # invalidate previous certs
    await ops_test.model.applications[charm_versions.tls.name].set_config(
        {"ca-common-name": "new-name"}
    )

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
