# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
from __future__ import annotations

import contextlib
import datetime
import json
import logging
import os
import re
import shutil
import subprocess
import uuid
import zipfile
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Generator, cast

import jubilant
import lightkube
import requests
import tomli
import tomli_w
import yaml
from lightkube.resources.core_v1 import Service
from spark8t.domain import PropertyFile
from spark_test.core.kyuubi import KyuubiClient

from constants import COS_METRICS_PORT, HA_ZNODE_NAME
from core.domain import Status

from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
ZOOKEEPER_NAME = "zookeeper-k8s"
ZOOKEEPER_PORT = 2181

PROCESS_NAME_PATTERN = "org.apache.kyuubi.server.KyuubiServer"
KYUUBI_CONTAINER_NAME = "kyuubi"

NODEPORT_MIN_VALUE = 30000
NODEPORT_MAX_VALUE = 32767
JDBC_PORT = 10009
JDBC_PORT_NAME = "kyuubi-jdbc"


def get_random_name():
    return str(uuid.uuid4()).replace("-", "_")


def get_leader_unit(juju: jubilant.Juju, app: str) -> str:
    """Get application leader unit."""
    status = juju.status()
    leader_unit = None
    for name, unit in status.apps[app].units.items():
        if unit.leader:
            leader_unit = name
    assert leader_unit, f"No leader unit found for {app}"
    return leader_unit


def fetch_connection_info(juju: jubilant.Juju, data_integrator: str) -> tuple[str, str, str]:
    """Return the JDBC endpoint and credentials for clients to connect to Kyuubi server."""
    logger.info("Running action 'get-credentials' on data-integrator unit...")
    task = juju.run(
        f"{data_integrator}/0",
        "get-credentials",
    )
    assert task.return_code == 0
    kyuubi_info = task.results["kyuubi"]
    return kyuubi_info["uris"], kyuubi_info["username"], kyuubi_info["password"]


def run_sql_test_against_jdbc_endpoint(
    juju: jubilant.Juju, test_pod: str, jdbc_endpoint: str, username: str, password: str
) -> bool:
    """Verify the JDBC endpoint exposed by the charm with some SQL queries."""
    database_name = get_random_name()
    table_name = get_random_name()
    logger.info(
        "Testing JDBC endpoint by connecting with beeline and executing a few SQL queries. "
        f"Using database {database_name} and table {table_name} ..."
    )
    process = subprocess.run(
        [
            "./tests/integration/test_jdbc_endpoint.sh",
            test_pod,
            cast(str, juju.model),
            jdbc_endpoint,
            get_random_name(),
            get_random_name(),
            username,
            password,
        ],
        capture_output=True,
    )
    print("========== test_jdbc_endpoint.sh STDOUT =================")
    print(process.stdout.decode())
    print("========== test_jdbc_endpoint.sh STDERR =================")
    print(process.stderr.decode())
    logger.info(f"JDBC endpoint test returned with status {process.returncode}")
    return process.returncode == 0


def get_zookeeper_quorum(juju: jubilant.Juju, zookeeper_name: str) -> str:
    addresses = []
    status = juju.status()
    for unit in status.apps[zookeeper_name].units.values():
        host = unit.address
        port = ZOOKEEPER_PORT
        addresses.append(f"{host}:{port}")
    return ",".join(addresses)


def get_active_kyuubi_servers_list(
    juju: jubilant.Juju, zookeeper_name=ZOOKEEPER_NAME
) -> list[str]:
    """Return the list of Kyuubi servers that are live in the cluster."""
    zookeeper_quorum = get_zookeeper_quorum(juju=juju, zookeeper_name=zookeeper_name)
    logger.info(f"Zookeeper quorum: {zookeeper_quorum}")
    pod_command = [
        "/opt/kyuubi/bin/kyuubi-ctl",
        "list",
        "server",
        "--zk-quorum",
        zookeeper_quorum,
        "--namespace",
        HA_ZNODE_NAME,
        "--version",
        "1.10.2",
    ]
    kubectl_command = [
        "kubectl",
        "exec",
        "kyuubi-k8s-0",
        "-c",
        "kyuubi",
        "-n",
        cast(str, juju.model),
        "--",
        *pod_command,
    ]

    process = subprocess.run(kubectl_command, capture_output=True, check=True)
    assert process.returncode == 0

    output_lines = process.stdout.decode().splitlines()
    pattern = r"\?\s+/kyuubi\s+\?\s+(?P<node>[\w\-.]+)\s+\?\s+(?P<port>\d+)\s+\?\s+(?P<version>[\d.]+-ubuntu[\d]+)\s+\?"
    servers = []

    for line in output_lines:
        match = re.match(pattern, line)
        if not match:
            continue
        servers.append(match.group("node"))

    return list(set(servers))


def delete_pod(pod_name: str, namespace: str) -> None:
    """Delete a pod with given name and namespace."""
    command = ["kubectl", "delete", "pod", pod_name, "-n", namespace]
    process = subprocess.run(command, capture_output=True, check=True)
    assert process.returncode == 0, f"Could not delete the pod {pod_name}."


def get_kyuubi_pid(juju: jubilant.Juju, unit: str) -> str | None:
    """Return the process ID of Kyuubi process in given pod."""
    pod_name = unit.replace("/", "-")
    command = [
        "kubectl",
        "exec",
        pod_name,
        "-c",
        KYUUBI_CONTAINER_NAME,
        "-n",
        cast(str, juju.model),
        "--",
        "ps",
        "aux",
    ]
    process = subprocess.run(command, capture_output=True, check=True)
    assert process.returncode == 0, (
        f"Command: {command} returned with return code {process.returncode}"
    )

    for line in process.stdout.decode().splitlines():
        match = re.search(re.escape(PROCESS_NAME_PATTERN), line)
        if match:
            pid = line.split()[1]
            logger.info(f"Found Kyuubi process with PID: {pid}")
            return pid
    return None


def kill_kyuubi_process(juju: jubilant.Juju, unit: str, kyuubi_pid: str) -> None:
    """Kill the Kyuubi process with given PID running in the given unit."""
    pod_name = unit.replace("/", "-")
    command = [
        "kubectl",
        "exec",
        pod_name,
        "-c",
        KYUUBI_CONTAINER_NAME,
        "-n",
        cast(str, juju.model),
        "--",
        "kill",
        "-SIGKILL",
        kyuubi_pid,
    ]
    process = subprocess.run(command, capture_output=True, check=True)
    assert process.returncode == 0, f"Could not kill Kyuubi process with pid {kyuubi_pid}."


def is_entire_cluster_responding_requests(
    juju: jubilant.Juju, test_pod: str, jdbc_endpoint: str, username: str, password: str
) -> bool:
    """Return whether the entire Kyuubi cluster is responding to requests from client."""
    status = juju.status()
    kyuubi_pods = {unit.replace("/", "-") for unit in status.apps[APP_NAME].units.keys()}
    logger.info(f"Nodes in the cluster being tested: {','.join(kyuubi_pods)}")
    pods_that_responded = set()

    tries = 0
    max_tries = 20
    command_executed_at = None

    while True:
        logger.info(f"Trying the {tries + 1}-th connection to see if entire cluster responds...")
        unique_id = get_random_name()
        query = f"SELECT '{unique_id}'"
        pod_command = [
            "/opt/kyuubi/bin/beeline",
            "-u",
            jdbc_endpoint,
            "-n",
            username,
            "-p",
            password,
            "-e",
            query,
        ]
        kubectl_command = [
            "kubectl",
            "exec",
            test_pod,
            "-n",
            cast(str, juju.model),
            "--",
            *pod_command,
        ]
        command_executed_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        logger.info(f"Executing command: {' '.join(kubectl_command)} at {command_executed_at}...")
        process = subprocess.run(kubectl_command, capture_output=True, check=True)
        assert process.returncode == 0

        for pod_name in kyuubi_pods:
            logs_command = [
                "kubectl",
                "logs",
                pod_name,
                "-n",
                cast(str, juju.model),
                "-c",
                "kyuubi",
                "--since-time",
                command_executed_at,
            ]
            logger.info(f"Checking pod logs for {pod_name}...")
            process = subprocess.run(logs_command, capture_output=True, check=True)
            assert process.returncode == 0

            pod_logs = process.stdout.decode()
            match = re.search(query, pod_logs)
            if match:
                logger.info(f"{pod_name} responded SUCCESS!")
                pods_that_responded.add(pod_name)
                break

        if pods_that_responded == kyuubi_pods:
            logger.info(f"All {len(kyuubi_pods)} nodes responded the requests.")
            return True

        if tries > max_tries:
            logger.warning(
                f"Tried for {tries} times, "
                f"but could only connect to {len(pods_that_responded)} nodes "
                f"({','.join(pods_that_responded)}) "
                f"out of {len(kyuubi_pods)} nodes "
                f"({','.join(kyuubi_pods)})"
            )
            break

        tries += 1

    return False


def prometheus_exporter_data(host: str) -> str | None:
    """Check if a given host has metric service available and it is publishing."""
    url = f"http://{host}:{COS_METRICS_PORT}/metrics"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        return None

    if response.status_code == 200:
        return response.text

    return None


def all_prometheus_exporters_data(juju: jubilant.Juju, check_field: str) -> bool:
    """Check if a all units has metric service available and publishing."""
    result = True
    status = juju.status()
    for unit in status.apps[APP_NAME].units.values():
        result = result and check_field in (prometheus_exporter_data(unit.address) or "")
    return result


def published_prometheus_alerts(juju: jubilant.Juju, host: str) -> dict:
    """Retrieve all Prometheus Alert rules that have been published."""
    if "http://" in host:
        host = host.split("//")[1]
    url = f"http://{host}/{cast(str, juju.model)}-prometheus-0/api/v1/rules"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        return {}

    if response.status_code == 200:
        return response.json()

    return {}


def published_prometheus_data(juju: jubilant.Juju, host: str, field: str) -> dict | None:
    """Check the existence of field among Prometheus published data."""
    if "http://" in host:
        host = host.split("//")[1]
    url = f"http://{host}/{cast(str, juju.model)}-prometheus-0/api/v1/query?query={field}"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        return None

    if response.status_code == 200:
        return response.json()

    return None


def published_grafana_dashboards(juju: jubilant.Juju) -> dict | None:
    """Get the list of dashboards published to Grafana."""
    base_url, pw = get_grafana_access(juju)
    url = f"{base_url}/api/search?query=&starred=false"

    try:
        session = requests.Session()
        session.auth = ("admin", pw)
        response = session.get(url)
    except requests.exceptions.RequestException:
        return None
    if response.status_code == 200:
        return response.json()

    return None


def published_loki_logs(
    juju: jubilant.Juju, field: str, value: str, limit: int = 300
) -> dict | None:
    """Get the list of dashboards published to Grafana."""
    base_url = get_cos_address(juju)
    url = f"{base_url}/{cast(str, juju.model)}-loki-0/loki/api/v1/query_range"

    params: dict[str, str | int] = {"query": f'{{{field}=~"{value}"}}', "limit": limit}
    try:
        response = requests.get(url, params=params)
    except requests.exceptions.RequestException:
        return None
    if response.status_code == 200:
        return response.json()

    return None


def get_cos_address(juju: jubilant.Juju) -> str:
    """Retrieve the URL where COS services are available."""
    task = juju.run("traefik/0", "show-proxied-endpoints")
    assert task.return_code == 0
    return json.loads(task.results["proxied-endpoints"])["traefik"]["url"]


def get_grafana_access(juju: jubilant.Juju) -> tuple[str, str]:
    """Get Grafana URL and password."""
    task = juju.run("grafana/0", "get-admin-password")
    assert task.return_code == 0
    return task.results["url"], task.results["admin-password"]


def deploy_minimal_kyuubi_setup(
    juju: jubilant.Juju,
    kyuubi_charm: str | Path,
    charm_versions: IntegrationTestsCharms,
    s3_bucket_and_creds: S3Info,
    trust: bool = True,
    num_units=1,
    integrate_zookeeper=False,
    deploy_from_charmhub=False,
) -> None:
    deploy_args = {
        "app": APP_NAME,
        "num_units": num_units,
        "channel": "edge",
        "base": "ubuntu@22.04",
        "trust": trust,
        # TODO(ga): Use stable revision
        "revision": 86,
    }
    if not deploy_from_charmhub:
        image_version = METADATA["resources"]["kyuubi-image"]["upstream-source"]
        resources = {"kyuubi-image": image_version}
        logger.info(f"Image version: {image_version}")

        deploy_args.update({"resources": resources})

    logger.info("Deploying kyuubi-k8s charm...")
    juju.deploy(kyuubi_charm, **deploy_args)

    logger.info("Waiting for kyuubi-k8s app to settle...")
    status = juju.wait(jubilant.all_blocked)
    logger.info(f"State of kyuubi-k8s app: {status.apps[APP_NAME]}")

    logger.info("Configuring kyuubi-k8s charm...")
    namespace = juju.model
    username = "kyuubi-spark-engine"
    charm_config = {"namespace": namespace, "service-account": username}
    juju.config(APP_NAME, charm_config)

    logger.info("Waiting for kyuubi-k8s app to settle...")
    status = juju.wait(jubilant.all_blocked)
    assert status.apps[APP_NAME].app_status.message == Status.MISSING_INTEGRATION_HUB.value.message

    logger.info("Deploying mandatory charms...")
    juju.deploy(**charm_versions.s3.deploy_dict())
    juju.deploy(**charm_versions.integration_hub.deploy_dict())
    juju.deploy(**charm_versions.auth_db.deploy_dict())

    logger.info("Waiting for s3-integrator app to be idle...")
    status = juju.wait(
        lambda status: jubilant.all_blocked(status, charm_versions.s3.app),
    )

    logger.info("Configuring s3-integrator...")
    endpoint_url = s3_bucket_and_creds["endpoint"]
    access_key = s3_bucket_and_creds["access_key"]
    secret_key = s3_bucket_and_creds["secret_key"]
    bucket_name = s3_bucket_and_creds["bucket"]
    path = s3_bucket_and_creds["path"]
    logger.info("Setting up s3 credentials in s3-integrator charm")
    task = juju.run(
        f"{charm_versions.s3.app}/0",
        "sync-s3-credentials",
        {"access-key": access_key, "secret-key": secret_key},
    )
    assert task.return_code == 0
    logger.info("Setting configuration for s3-integrator charm...")
    juju.config(
        charm_versions.s3.app,
        {
            "bucket": bucket_name,
            "path": path,
            "endpoint": endpoint_url,
        },
    )
    logger.info("Waiting for s3-integrator app to be idle and active...")
    juju.wait(lambda status: jubilant.all_active(status, charm_versions.s3.app))

    logger.info("Waiting for integration_hub and s3-integrator app to be idle and active...")
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.s3.app,
            charm_versions.integration_hub.app,
        )
    )

    logger.info("Integrating integration-hub charm with s3-integrator charm...")
    juju.integrate(charm_versions.s3.app, charm_versions.integration_hub.app)

    logger.info("Waiting for s3-integrator and integration-hub charms to be idle and active...")
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.s3.app,
            charm_versions.integration_hub.app,
        ),
        delay=5,
    )

    # Add configuration key to prevent resource starvation during tests
    task = juju.run(
        f"{charm_versions.integration_hub.app}/0",
        "add-config",
        {"conf": "spark.kubernetes.executor.request.cores=0.1"},
    )
    assert task.return_code == 0

    logger.info("Integrating kyuubi charm with integration-hub charm...")
    juju.integrate(charm_versions.integration_hub.app, APP_NAME)

    logger.info("Waiting for s3-integrator and integration_hub charms to be idle and active...")
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.s3.app,
            charm_versions.integration_hub.app,
        ),
        delay=5,
    )

    logger.info("Waiting for auth-db charm to be idle and active...")
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.auth_db.app,
        ),
        delay=10,
        timeout=2000,
    )
    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm...")
    juju.integrate(charm_versions.auth_db.application_name, f"{APP_NAME}:auth-db")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.auth_db.app,
        ),
        delay=15,
        timeout=1000,
    )

    if integrate_zookeeper:
        # Deploy Zookeeper and wait
        juju.deploy(**charm_versions.zookeeper.deploy_dict())
        logger.info("Waiting for zookeeper-k8s charm to be active and idle...")
        juju.wait(
            lambda status: jubilant.all_active(
                status,
                charm_versions.zookeeper.app,
            ),
        )

        # Integrate Kyuubi with Zookeeper and wait
        logger.info("Integrating kyuubi charm with zookeeper charm...")
        juju.integrate(charm_versions.zookeeper.app, APP_NAME)
        logger.info(
            "Waiting for s3-integrator, integration_hub and zookeeper to be idle and active..."
        )
        juju.wait(
            lambda status: jubilant.all_active(
                status,
                charm_versions.s3.app,
                charm_versions.integration_hub.app,
                charm_versions.zookeeper.app,
            ),
            delay=5,
        )

    juju.deploy(**charm_versions.data_integrator.deploy_dict(), config={"database-name": "test"})
    logger.info("Waiting for data-integrator charm to be idle...")
    juju.wait(lambda status: jubilant.all_blocked(status, charm_versions.data_integrator.app))
    logger.info("Integrating kyuubi charm with zookeeper charm...")
    juju.integrate(charm_versions.data_integrator.app, APP_NAME)

    logger.info("Successfully deployed minimal working Kyuubi setup.")


def get_k8s_service(namespace: str, service_name: str) -> Service | None:
    client = lightkube.Client()
    try:
        service = client.get(
            res=lightkube.resources.core_v1.Service,
            name=service_name,
            namespace=namespace,
        )
    except lightkube.core.exceptions.ApiError as e:
        if e.status.code == 404:
            return None
        raise

    return service


def run_command_in_pod(
    juju: jubilant.Juju,
    pod_name: str,
    pod_command: list[str],
) -> tuple[str, str]:
    """Load certificate in the pod."""
    kubectl_command = [
        "kubectl",
        "exec",
        pod_name,
        "-c",
        "kyuubi",
        "-n",
        cast(str, juju.model),
        "--",
        *pod_command,
    ]
    process = subprocess.run(kubectl_command, capture_output=True, check=True)
    stdout = process.stdout.decode()
    stderr = process.stderr.decode()
    logger.info(stdout)
    logger.info(stderr)
    assert process.returncode == 0
    return stdout, stderr


def umask_named_temporary_file(*args, **kargs):
    """Return a temporary file descriptor readable by all users."""
    file_desc = NamedTemporaryFile(*args, **kargs)
    mask = os.umask(0o666)
    os.umask(mask)
    os.chmod(file_desc.name, 0o666 & ~mask)
    return file_desc


def assert_service_status(
    namespace: str,
    service_type: str,
) -> Service:
    """Utility function to check status of managed K8s service created by Kyuubi charm."""
    service_name = f"{APP_NAME}-service"
    service = get_k8s_service(namespace=namespace, service_name=service_name)
    logger.info(f"{service=}")

    assert service is not None

    service_spec = service.spec
    assert service_spec is not None
    assert service_type == service_spec.type
    assert service_spec.selector == {"app.kubernetes.io/name": APP_NAME}

    assert service_spec.ports is not None
    service_port = service_spec.ports[0]
    assert service_port is not None
    assert service_port.port == JDBC_PORT
    assert service_port.targetPort == JDBC_PORT
    assert service_port.name == JDBC_PORT_NAME
    assert service_port.protocol == "TCP"

    if service_type in ("NodePort", "LoadBalancer"):
        assert service_port.nodePort is not None
        assert NODEPORT_MIN_VALUE <= int(service_port.nodePort) <= NODEPORT_MAX_VALUE

    return service


def fetch_spark_properties(juju: jubilant.Juju, unit_name: str) -> dict[str, str]:
    pod_name = unit_name.replace("/", "-")
    command = ["cat", "/etc/spark8t/conf/spark-defaults.conf"]
    stdout, _ = run_command_in_pod(juju, pod_name=pod_name, pod_command=command)
    with NamedTemporaryFile(mode="w+") as temp_file:
        temp_file.write(stdout)
        temp_file.seek(0)
        props = PropertyFile.read(temp_file.name).props
        return props


def validate_sql_queries_with_kyuubi(
    juju: jubilant.Juju,
    kyuubi_host: str | None = None,
    kyuubi_port: str | int = 10009,
    username: str | None = None,
    password: str | None = None,
    query_lines: list[str] | None = None,
    db_name: str | None = None,
    table_name: str | None = None,
    use_tls: bool = False,
):
    """Run simple SQL queries to validate Kyuubi and return whether this validation is successful."""
    if not kyuubi_host:
        kyuubi_host = juju.status().apps[APP_NAME].units[f"{APP_NAME}/0"].address
        logger.info(f"Reaching out to kyuubi on {kyuubi_host}")
        response = subprocess.check_output(
            f"openssl s_client -showcerts -connect {kyuubi_host}:10009 < /dev/null || true",
            stderr=subprocess.PIPE,
            shell=True,
            universal_newlines=True,
        )
        logger.info(response)
    if not db_name:
        db_name = str(uuid.uuid4()).replace("-", "_")
    if not table_name:
        table_name = str(uuid.uuid4()).replace("-", "_")
    if not query_lines:
        query_lines = [
            f"CREATE DATABASE `{db_name}`; ",
            f"USE `{db_name}`; ",
            f"CREATE TABLE `{table_name}` (id INT); ",
            f"INSERT INTO `{table_name}` VALUES (12345); ",
            f"SELECT * FROM `{table_name}`; ",
        ]
    args = {"host": kyuubi_host, "port": int(kyuubi_port)}
    if username:
        args.update({"username": username})
    if password:
        args.update({"password": password})
    kyuubi_client = KyuubiClient(**args, use_ssl=use_tls)

    with kyuubi_client.connection as conn, conn.cursor() as cursor:
        for line in query_lines:
            cursor.execute(line)
        results = cursor.fetchall()
        return len(results) == 1


@contextlib.contextmanager
def inject_dependency_fault(original_charm_file: Path) -> Generator[Path, None, None]:
    """Inject a dependency fault into the Kyuubi charm."""
    filename = Path(original_charm_file).name
    tmp = Path("tmp")
    tmp.mkdir(exist_ok=True)
    fault_charm = tmp / filename
    shutil.copy(original_charm_file, fault_charm)

    logger.info("Inject dependency fault")
    with Path("refresh_versions.toml").open("rb") as file:
        versions = tomli.load(file)

    versions["charm"] = "1/0.0.0"  # Let's use a track that does not exist

    # Overwrite refresh_versions.toml with incompatible version.
    with zipfile.ZipFile(fault_charm, mode="a") as charm_zip:
        charm_zip.writestr("refresh_versions.toml", tomli_w.dumps(versions))

    yield fault_charm

    fault_charm.unlink(missing_ok=True)
    tmp.rmdir()
