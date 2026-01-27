#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import ast
import base64
import json
import logging
import re
import ssl
import subprocess
from pathlib import Path
from typing import cast

import jubilant
import pytest
import yaml
from thrift.transport.TTransport import TTransportException

from core.domain import Status

from .helpers import (
    assert_service_status,
    deploy_minimal_kyuubi_setup,
    fetch_connection_info,
    kyuubi_host_port_from_jdbc_uri,
    mock_hostname_resolution,
    validate_sql_queries_with_kyuubi,
    verify_certificate_matches_public_key,
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
    """A fixture that returns a private-public key pair."""
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


def test_enable_tls(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
) -> None:
    """Enable TLS and check the server responds with the certificate."""
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
    """Test the endpoint exposed by Kyuubi while TLS is enabled."""
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
    """Test the endpoint exposed by Kyuubi on different types like NodePort and LoadBalancer."""
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
    """Renew the TLS certificate and check the Kyuubi connection."""
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


def test_set_tls_private_key_secret_not_granted(
    juju: jubilant.Juju, rsa_key_pair: tuple[bytes, bytes]
) -> None:
    """Set tls-client-private-key config option to a secret which is not granted."""
    private_key, public_key = rsa_key_pair
    secret_name = "tls-key-not-granted"
    secret_uri = juju.add_secret(secret_name, {"private-key": private_key.decode()})
    juju.config(APP_NAME, {"tls-client-private-key": secret_uri})
    juju.wait(jubilant.all_agents_idle, delay=5)
    juju.wait(lambda status: jubilant.all_blocked(status, APP_NAME), delay=5)
    status = juju.status()
    assert (
        status.apps[APP_NAME].app_status.message
        == Status.TLS_SECRET_INSUFFICIENT_PERMISSION.value.message
    )


def test_set_tls_private_key_secret_not_valid(
    juju: jubilant.Juju, rsa_key_pair: tuple[bytes, bytes]
) -> None:
    """Set tls-client-private-key config option to a secret which is not valid."""
    private_key, public_key = rsa_key_pair
    secret_name = "tls-key-invalid-1"
    secret_uri = juju.add_secret(secret_name, {"foo-bar": private_key.decode()})
    juju.cli("grant-secret", secret_name, APP_NAME)
    juju.config(APP_NAME, {"tls-client-private-key": secret_uri})
    juju.wait(jubilant.all_agents_idle, delay=5)
    juju.wait(lambda status: jubilant.all_blocked(status, APP_NAME), delay=5)
    status = juju.status()
    assert status.apps[APP_NAME].app_status.message == Status.TLS_SECRET_INVALID.value.message

    secret_name = "tls-key-invalid-2"
    # Reverse the string, should obviously be an invalid private key
    key_content = private_key.decode()[::-1]
    secret_uri = juju.add_secret(secret_name, {"private-key": key_content})
    juju.cli("grant-secret", secret_name, APP_NAME)
    juju.config(APP_NAME, {"tls-client-private-key": secret_uri})
    juju.wait(jubilant.all_agents_idle, delay=5)
    juju.wait(lambda status: jubilant.all_blocked(status, APP_NAME), delay=5)
    status = juju.status()
    assert status.apps[APP_NAME].app_status.message == Status.TLS_SECRET_INVALID.value.message


def test_set_tls_private_key_valid(
    juju: jubilant.Juju, charm_versions, rsa_key_pair: tuple[bytes, bytes], context
) -> None:
    """Set tls-client-private-key config option to a secret that's valid, and test the Kyuubi connection."""
    private_key, public_key = rsa_key_pair
    secret_name = "tls-key-valid"
    secret_uri = juju.add_secret(secret_name, {"private-key": private_key.decode()})
    juju.cli("grant-secret", secret_name, APP_NAME)
    juju.config(APP_NAME, {"tls-client-private-key": secret_uri})
    context["tls_private_key_secret_name"] = secret_name
    juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status), delay=10
    )

    jdbc_uri, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    kyuubi_host, kyuubi_port = kyuubi_host_port_from_jdbc_uri(jdbc_uri=jdbc_uri)

    # Verify that the server now presents a server certificate that corresponds to the
    # keypair that was supplied to the charm.
    server_cert = ssl.get_server_certificate((kyuubi_host, kyuubi_port)).encode()
    assert verify_certificate_matches_public_key(certificate=server_cert, public_key=public_key)

    # Try connecting to Kyuubi using the CA certificate
    ca_cert = fetch_ca_certificate(juju, f"{charm_versions.tls.app}/0")
    assert validate_sql_queries_with_kyuubi(
        juju=juju,
        kyuubi_host=kyuubi_host,
        kyuubi_port=kyuubi_port,
        username=username,
        password=password,
        use_tls=True,
        ca_cert=ca_cert,
    )


def test_update_tls_private_key(
    juju: jubilant.Juju, charm_versions, rsa_key_pair: tuple[bytes, bytes], context
) -> None:
    """Update the TLS private key set by the user and test Kyuubi connection."""
    private_key, public_key = rsa_key_pair
    secret_name = context.pop("tls_private_key_secret_name")
    b64encoded_private_key = base64.b64encode(private_key).decode()
    juju.cli("update-secret", secret_name, f"private-key={b64encoded_private_key}")
    context["public_key"] = public_key
    juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status), delay=10
    )

    jdbc_uri, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    kyuubi_host, kyuubi_port = kyuubi_host_port_from_jdbc_uri(jdbc_uri=jdbc_uri)

    # Verify that the server now presents a server certificate that corresponds to the
    # keypair that was supplied to the charm.
    server_cert = ssl.get_server_certificate((kyuubi_host, kyuubi_port)).encode()
    assert verify_certificate_matches_public_key(certificate=server_cert, public_key=public_key)
    context["server_cert"] = server_cert

    # Try connecting to Kyuubi using the CA certificate
    ca_cert = fetch_ca_certificate(juju, f"{charm_versions.tls.app}/0")
    assert validate_sql_queries_with_kyuubi(
        juju=juju,
        kyuubi_host=kyuubi_host,
        kyuubi_port=kyuubi_port,
        username=username,
        password=password,
        use_tls=True,
        ca_cert=ca_cert,
    )


def test_renew_certificate_using_users_private_key(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, context
) -> None:
    """Test renewal of TLS certificate while the user has configured custom TLS private key."""
    old_ca_cert = fetch_ca_certificate(juju, f"{charm_versions.tls.app}/0")

    # invalidate previous certs
    juju.config(charm_versions.tls.app, {"ca-common-name": "new-name-again"})
    juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status), delay=10
    )
    jdbc_uri, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    kyuubi_host, kyuubi_port = kyuubi_host_port_from_jdbc_uri(jdbc_uri=jdbc_uri)

    response = subprocess.check_output(
        f"openssl s_client -showcerts -connect {kyuubi_host}:{kyuubi_port} < /dev/null",
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True,
    )

    assert "TLSv1.3" in response
    assert re.search(r"CN\s?=\s?new-name-again", response)

    # Verify that the new certificate presented by the server should also correspond to the
    # kep-pair configured by the user.
    server_cert = ssl.get_server_certificate((kyuubi_host, kyuubi_port)).encode()
    old_server_cert = context.pop("server_cert")
    assert server_cert != old_server_cert
    old_public_key = context["public_key"]
    assert verify_certificate_matches_public_key(
        certificate=server_cert, public_key=old_public_key
    )

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


def test_remove_tls_private_key_config(juju: jubilant.Juju, charm_versions, context) -> None:
    """Test removal of tls-client-private-key config option."""
    juju.config(APP_NAME, reset="tls-client-private-key")
    juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status), delay=10
    )

    jdbc_uri, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    kyuubi_host, kyuubi_port = kyuubi_host_port_from_jdbc_uri(jdbc_uri=jdbc_uri)

    # Verify that the server now presents a server certificate that does not match with the old
    # TLS private key. In other words, the charm should create new private key and use that instead.
    old_public_key = context.pop("public_key")
    server_cert = ssl.get_server_certificate((kyuubi_host, kyuubi_port)).encode()
    assert not verify_certificate_matches_public_key(
        certificate=server_cert, public_key=old_public_key
    )

    # Try connecting to Kyuubi using the CA certificate
    ca_cert = fetch_ca_certificate(juju, f"{charm_versions.tls.app}/0")
    assert validate_sql_queries_with_kyuubi(
        juju=juju,
        kyuubi_host=kyuubi_host,
        kyuubi_port=kyuubi_port,
        username=username,
        password=password,
        use_tls=True,
        ca_cert=ca_cert,
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
