#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import ast
import asyncio
import json
import logging
import os
import re
import subprocess
from pathlib import Path

import pytest
import yaml
from juju.application import Application
from juju.unit import Unit
from ops import StatusBase
from pytest_operator.plugin import OpsTest

from constants import TLS_REL
from core.domain import Status

from .helpers import (
    assert_service_status,
    deploy_minimal_kyuubi_setup,
    find_leader_unit,
    get_address,
    run_command_in_pod,
    run_sql_test_against_jdbc_endpoint,
    umask_named_temporary_file,
)

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
    ops_test: OpsTest, kyuubi_charm: Path, charm_versions, s3_bucket_and_creds
) -> None:
    """Deploy minimal Kyuubi deployments."""
    """Test the status of default managed K8s service when Kyuubi is deployed."""
    await deploy_minimal_kyuubi_setup(
        ops_test=ops_test,
        kyuubi_charm=kyuubi_charm,
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=True,
        num_units=1,
        integrate_zookeeper=False,
    )

    # Wait for everything to settle down
    await ops_test.model.wait_for_idle(
        apps=[
            APP_NAME,
            charm_versions.integration_hub.application_name,
            # charm_versions.zookeeper.application_name,
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
        "Testing JDBC endpoint by connecting with beeline and executing a few SQL queries..."
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

    assert await run_sql_test_against_jdbc_endpoint(ops_test, test_pod, username, password)


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
    assert re.search(r"CN\s?=\s?kyuubi", response)

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
    ca_cert = certificates["ca"]

    logger.info(f"Copy the CA certificate to the testpod in this location: {CERTIFICATE_LOCATION}")
    with umask_named_temporary_file(
        mode="w",
        prefix="cert-",
        suffix=".conf",
        dir=os.path.expanduser("~"),
    ) as temp_file:
        with open(temp_file.name, "w+") as f:
            f.writelines(ca_cert)
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
        "Testing JDBC endpoint by connecting with beeline and executing a few SQL queries..."
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

    assert await run_sql_test_against_jdbc_endpoint(
        ops_test, test_pod, username, password, jdbc_endpoint=jdbc_endpoint_ssl
    )


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
    assert "CN = new-name" in response


@pytest.mark.abort_on_fail
async def test_loadbalancer_service(
    ops_test,
    test_pod,
    charm_versions,
):
    """Test the tls connection with loadbalancer."""
    logger.info("Changing expose-external to 'loadbalancer' for kyuubi-k8s charm...")
    await ops_test.model.applications[APP_NAME].set_config({"expose-external": "loadbalancer"})

    logger.info("Waiting for kyuubi-k8s app to be active and idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )

    assert_service_status(namespace=ops_test.model_name, service_type="LoadBalancer")

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

    c1 = ["rm", TRUSTSTORE_LOCATION]
    await run_command_in_pod(ops_test, "testpod", c1)

    logger.info(f"Generating the trustore in the testpod in this location: {TRUSTSTORE_LOCATION}")
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
        "Testing JDBC endpoint by connecting with beeline and executing a few SQL queries..."
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

    assert await run_sql_test_against_jdbc_endpoint(
        ops_test, test_pod, username, password, jdbc_endpoint=jdbc_endpoint_ssl
    )


@pytest.mark.abort_on_fail
async def test_disable_tls(ops_test: OpsTest, charm_versions):
    """Test that we are able to disable TLS by removing the certificates relation."""
    await ops_test.model.applications[APP_NAME].remove_relation(
        TLS_REL, f"{charm_versions.tls.name}:{TLS_REL}"
    )

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            timeout=1000,
            idle_period=30,
        )

    host = await get_address(ops_test, unit_name=f"{APP_NAME}/0")
    response = subprocess.check_output(
        f"openssl s_client -showcerts -connect {host}:10009 < /dev/null || true",
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True,
    )

    assert "No client certificate CA names sent" in response
