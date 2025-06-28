#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import ast
import json
import logging
import re
import subprocess
from pathlib import Path
from typing import cast

import jubilant
import pytest
import yaml
from thrift.transport.TTransport import TTransportException

from .helpers import (
    assert_service_status,
    deploy_minimal_kyuubi_setup,
    fetch_connection_info,
    kyuubi_host_port_from_jdbc_uri,
    mock_hostname_resolution,
    validate_sql_queries_with_kyuubi,
)
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

TRUSTSTORE_PASSWORD = "password"
CERTIFICATE_LOCATION = "/tmp/cert.pem"
TRUSTSTORE_LOCATION = "/tmp/truststore.jks"


def fetch_ca_certificate(juju: jubilant.Juju, unit_name: str) -> str:
    """Fetch CA certificate from self-signed-certificates operator."""
    logger.info("Get certificate from self-signed certificates operator")
    task = juju.run(unit_name, "get-issued-certificates")
    assert task.return_code == 0
    items = ast.literal_eval(task.results.get("certificates", "[]"))
    certificates = json.loads(items[0])
    ca_cert = certificates.get("ca", "")
    return ca_cert


@pytest.fixture
def rsa_key_pair(tmp_path: Path) -> tuple[bytes, bytes]:
    private_key = tmp_path / "private.key"
    public_key = tmp_path / "public.key.pub"
    subprocess.run(["openssl", "genrsa", "-out", private_key, "2048"], check=True)
    subprocess.run(
        ["openssl", "rsa", "-in", private_key, "-pubout", "-out", public_key], check=True
    )
    with open(private_key, "rb") as private, open(public_key, "rb") as public:
        return private.read(), public.read()


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
            charm_versions.data_integrator.app,
            charm_versions.integration_hub.app,
            charm_versions.s3.app,
            charm_versions.data_integrator.app,
        ),
        delay=5,
    )


def test_jdbc_endpoint_without_tls(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Test the JDBC endpoint exposed by the charm."""
    logger.info(
        "Testing JDBC endpoint by connecting with beeline and executing a few SQL queries..."
    )
    _, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    assert validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)


def test_enable_ssl(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
) -> None:
    juju.deploy(
        **charm_versions.tls.deploy_dict(),
        config={"ca-common-name": "kyuubi"},
    )
    juju.wait(jubilant.all_active, delay=5)
    juju.integrate(APP_NAME, charm_versions.tls.app)
    status = juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status), delay=10
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


def test_jdbc_endpoint_with_tls_enabled(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
):
    ca_cert = fetch_ca_certificate(juju, f"{charm_versions.tls.app}/0")
    jdbc_uri, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    kyuubi_unit_ip = juju.status().apps[APP_NAME].units[f"{APP_NAME}/0"].address
    jdbc_host, _ = kyuubi_host_port_from_jdbc_uri(jdbc_uri=jdbc_uri)

    # Ensure that connection fails without using TLS
    with mock_hostname_resolution(jdbc_host, kyuubi_unit_ip):
        with pytest.raises(TTransportException):
            validate_sql_queries_with_kyuubi(
                juju=juju, jdbc_uri=jdbc_uri, username=username, password=password, use_tls=False
            )

    # Ensure that connection fails without using TLS
    with mock_hostname_resolution(jdbc_host, kyuubi_unit_ip):
        validate_sql_queries_with_kyuubi(
            juju=juju,
            jdbc_uri=jdbc_uri,
            username=username,
            password=password,
            use_tls=True,
            ca_cert=ca_cert,
        )


@pytest.mark.parametrize(
    "expose_external,service_type", [("nodeport", "NodePort"), ("loadbalancer", "LoadBalancer")]
)
def test_tls_with_external_exposure(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    expose_external: str,
    service_type: str,
):
    logger.info(f"Changing expose-external to '{expose_external}' for kyuubi-k8s charm...")
    juju.config(APP_NAME, {"expose-external": expose_external})

    logger.info("Waiting for apps to be active and idle...")
    juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status), delay=10
    )
    assert_service_status(namespace=cast(str, juju.model), service_type=service_type)

    ca_cert = fetch_ca_certificate(juju, f"{charm_versions.tls.app}/0")
    jdbc_uri, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)

    # Ensure that connection fails without using TLS
    with pytest.raises(TTransportException):
        validate_sql_queries_with_kyuubi(
            juju=juju, jdbc_uri=jdbc_uri, username=username, password=password, use_tls=False
        )

    # Ensure that the connection succeeds with TLS
    assert validate_sql_queries_with_kyuubi(
        juju=juju,
        jdbc_uri=jdbc_uri,
        username=username,
        password=password,
        use_tls=True,
        ca_cert=ca_cert,
    )


def test_kill_pod_and_verify_tls_on_new_pod(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
) -> None:
    """Check that a unit spawned by the stateful set still triggers the required events to setup tls."""
    subprocess.check_output(["kubectl", "delete", "pod", f"{APP_NAME}-0", "-n", str(juju.model)])
    juju.wait(jubilant.all_active, timeout=300, delay=10)

    ca_cert = fetch_ca_certificate(juju, f"{charm_versions.tls.app}/0")
    jdbc_uri, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)

    assert validate_sql_queries_with_kyuubi(
        juju=juju,
        jdbc_uri=jdbc_uri,
        username=username,
        password=password,
        use_tls=True,
        ca_cert=ca_cert,
    )


def test_renew_cert(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    old_ca_cert = fetch_ca_certificate(juju, f"{charm_versions.tls.app}/0")

    # invalidate previous certs
    juju.config(charm_versions.tls.app, {"ca-common-name": "new-name"})
    status = juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status), delay=10
    )
    # check client-presented certs
    host = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address

    response = subprocess.check_output(
        f"openssl s_client -showcerts -connect {host}:10009 < /dev/null",
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True,
    )

    assert "TLSv1.3" in response
    assert re.search(r"CN\s?=\s?new-name", response)

    new_ca_cert = fetch_ca_certificate(juju, f"{charm_versions.tls.app}/0")
    jdbc_uri, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)

    # Ensure that connection fails without using TLS
    with pytest.raises(TTransportException):
        validate_sql_queries_with_kyuubi(
            juju=juju, jdbc_uri=jdbc_uri, username=username, password=password, use_tls=False
        )

    # Ensure that connection fails using old TLS cert
    with pytest.raises(TTransportException):
        validate_sql_queries_with_kyuubi(
            juju=juju,
            jdbc_uri=jdbc_uri,
            username=username,
            password=password,
            use_tls=True,
            ca_cert=old_ca_cert,
        )

    # Ensure that the connection succeeds with TLS
    assert validate_sql_queries_with_kyuubi(
        juju=juju,
        jdbc_uri=jdbc_uri,
        username=username,
        password=password,
        use_tls=True,
        ca_cert=new_ca_cert,
    )


def test_disable_tls(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    """Test that we are able to disable TLS by removing the certificates relation."""
    juju.remove_relation(APP_NAME, charm_versions.tls.app)

    status = juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status), delay=10
    )

    host = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address

    response = subprocess.check_output(
        f"openssl s_client -showcerts -connect {host}:10009 < /dev/null || true",
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True,
    )

    assert "No client certificate CA names sent" in response

    jdbc_uri, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)

    # Ensure that the connection succeeds without TLS
    assert validate_sql_queries_with_kyuubi(
        juju=juju, jdbc_uri=jdbc_uri, username=username, password=password, use_tls=False
    )
