#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import ast
import json
import logging
import os
import re
import subprocess
from pathlib import Path
from typing import cast

import jubilant
import yaml

from .helpers import (
    assert_service_status,
    deploy_minimal_kyuubi_setup,
    fetch_password,
    run_command_in_pod,
    run_sql_test_against_jdbc_endpoint,
    umask_named_temporary_file,
)
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

TRUSTSTORE_PASSWORD = "password"
CERTIFICATE_LOCATION = "/tmp/cert.pem"
TRUSTSTORE_LOCATION = "/tmp/truststore.jks"


def test_build_and_deploy(
    juju: jubilant.Juju,
    kyuubi_charm: Path,
    charm_versions: IntegrationTestsCharms,
    s3_bucket_and_creds: S3Info,
) -> None:
    """Deploy minimal Kyuubi deployments."""
    """Test the status of default managed K8s service when Kyuubi is deployed."""
    deploy_minimal_kyuubi_setup(
        juju=juju,
        kyuubi_charm=kyuubi_charm,
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=True,
        num_units=1,
        integrate_zookeeper=False,
    )

    # Wait for everything to settle down
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            APP_NAME,
            charm_versions.integration_hub.app,
            charm_versions.s3.app,
        ),
        delay=5,
    )


def test_jdbc_endpoint_with_default_metastore(juju: jubilant.Juju, test_pod: str) -> None:
    """Test the JDBC endpoint exposed by the charm."""
    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")

    task = juju.run(f"{APP_NAME}/0", "get-jdbc-endpoint")
    assert task.return_code == 0
    jdbc_endpoint = task.results["endpoint"]
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    logger.info(
        "Testing JDBC endpoint by connecting with beeline and executing a few SQL queries..."
    )
    username = "admin"
    password = fetch_password(juju)

    assert run_sql_test_against_jdbc_endpoint(
        juju, test_pod, jdbc_endpoint, username=username, password=password
    )


def test_enable_ssl(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, test_pod: str
) -> None:
    juju.deploy(
        **charm_versions.tls.deploy_dict(),
        config={"ca-common-name": "kyuubi"},
    )
    juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME, charm_versions.tls.app), delay=5
    )
    juju.integrate(APP_NAME, charm_versions.tls.app)
    status = juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME, charm_versions.tls.app), delay=10
    )

    host = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address

    response = subprocess.check_output(
        f"openssl s_client -showcerts -connect {host}:10009 < /dev/null || true",
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True,
    )

    assert "TLSv1.3" in response
    assert re.search(r"CN\s?=\s?kyuubi", response)

    # get issued certificates
    logger.info("Get certificate from self-signed certificate operator")
    task = juju.run(f"{charm_versions.tls.app}/0", "get-issued-certificates")
    assert task.return_code == 0
    items = ast.literal_eval(task.results.get("certificates", "[]"))
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
            cast(str, juju.model),
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

    run_command_in_pod(juju, "testpod", c2)
    # mod permission of the trustore
    c3 = ["chmod", "u+x", TRUSTSTORE_LOCATION]
    run_command_in_pod(juju, "testpod", c3)

    # run query with tls
    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    task = juju.run(f"{APP_NAME}/0", "get-jdbc-endpoint")
    assert task.return_code == 0

    jdbc_endpoint = task.results["endpoint"]
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    jdbc_endpoint_ssl = (
        jdbc_endpoint
        + f";ssl=true;trustStorePassword={TRUSTSTORE_PASSWORD};sslTrustStore={TRUSTSTORE_LOCATION}"
    )
    logger.info(f"JDBC endpoint with SSL: {jdbc_endpoint_ssl}")

    logger.info(
        "Testing JDBC endpoint by connecting with beeline and executing a few SQL queries..."
    )
    username = "admin"
    password = fetch_password(juju)

    assert run_sql_test_against_jdbc_endpoint(
        juju, test_pod, jdbc_endpoint=jdbc_endpoint_ssl, username=username, password=password
    )


def test_renew_cert(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    # invalidate previous certs
    juju.config(charm_versions.tls.app, {"ca-common-name": "new-name"})
    status = juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=10)

    # check client-presented certs
    host = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address

    response = subprocess.check_output(
        f"openssl s_client -showcerts -connect {host}:10009 < /dev/null",
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True,
    )

    assert "TLSv1.3" in response
    assert "CN = new-name" in response


def test_loadbalancer_service(
    juju: jubilant.Juju,
    test_pod: str,
    charm_versions: IntegrationTestsCharms,
) -> None:
    """Test the tls connection with loadbalancer."""
    logger.info("Changing expose-external to 'loadbalancer' for kyuubi-k8s charm...")
    juju.config(APP_NAME, {"expose-external": "loadbalancer"})

    logger.info("Waiting for kyuubi-k8s app to be active and idle...")
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=10)

    assert_service_status(namespace=cast(str, juju.model), service_type="LoadBalancer")

    # get issued certificates
    logger.info("Get certificate from self-signed certificate operator")
    task = juju.run(f"{charm_versions.tls.app}/0", "get-issued-certificates")
    assert task.return_code == 0
    items = ast.literal_eval(task.results.get("certificates", "[]"))
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
            cast(str, juju.model),
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
    run_command_in_pod(juju, "testpod", c1)

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

    run_command_in_pod(juju, "testpod", c2)
    # mod permission of the trustore
    c3 = ["chmod", "u+x", TRUSTSTORE_LOCATION]
    run_command_in_pod(juju, "testpod", c3)

    # run query with tls
    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    task = juju.run(f"{APP_NAME}/0", "get-jdbc-endpoint")
    assert task.return_code == 0
    jdbc_endpoint = task.results["endpoint"]
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    jdbc_endpoint_ssl = (
        jdbc_endpoint
        + f";ssl=true;trustStorePassword={TRUSTSTORE_PASSWORD};sslTrustStore={TRUSTSTORE_LOCATION}"
    )
    logger.info(f"JDBC endpoint with SSL: {jdbc_endpoint_ssl}")

    logger.info(
        "Testing JDBC endpoint by connecting with beeline and executing a few SQL queries..."
    )
    username = "admin"
    password = fetch_password(juju)
    assert run_sql_test_against_jdbc_endpoint(
        juju, test_pod, jdbc_endpoint=jdbc_endpoint_ssl, username=username, password=password
    )


def test_disable_tls(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    """Test that we are able to disable TLS by removing the certificates relation."""
    juju.remove_relation(APP_NAME, charm_versions.tls.app)

    status = juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=10)

    host = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address

    response = subprocess.check_output(
        f"openssl s_client -showcerts -connect {host}:10009 < /dev/null || true",
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True,
    )

    assert "No client certificate CA names sent" in response
