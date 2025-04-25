from __future__ import annotations

import datetime
import json
import logging
import os
import re
import subprocess
import uuid
from pathlib import Path
from subprocess import PIPE, check_output
from tempfile import NamedTemporaryFile
from typing import List

import jubilant
import lightkube
import requests
import yaml
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


# def check_status(entity: Application | Unit, status: StatusBase):
#     if isinstance(entity, Application):
#         return entity.status == status.name and entity.status_message == status.message
#     elif isinstance(entity, Unit):
#         return (
#             entity.workload_status == status.name
#             and entity.workload_status_message == status.message
#         )
#     else:
#         raise ValueError(f"entity type {type(entity)} is not allowed")


async def fetch_jdbc_endpoint(ops_test):
    """Return the JDBC endpoint for clients to connect to Kyuubi server."""
    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    for unit in ops_test.model.applications[APP_NAME].units:
        if await unit.is_leader_from_status():
            kyuubi_unit = unit
    action = await kyuubi_unit.run_action(
        action_name="get-jdbc-endpoint",
    )
    result = await action.wait()

    jdbc_endpoint = result.results.get("endpoint")
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    return jdbc_endpoint


async def run_sql_test_against_jdbc_endpoint(ops_test: OpsTest, test_pod, jdbc_endpoint=None):
    """Verify the JDBC endpoint exposed by the charm with some SQL queries."""
    if jdbc_endpoint is None:
        jdbc_endpoint = await fetch_jdbc_endpoint(ops_test)
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
            ops_test.model_name,
            jdbc_endpoint,
            get_random_name(),
            get_random_name(),
        ],
        capture_output=True,
    )
    print("========== test_jdbc_endpoint.sh STDOUT =================")
    print(process.stdout.decode())
    print("========== test_jdbc_endpoint.sh STDERR =================")
    print(process.stderr.decode())
    logger.info(f"JDBC endpoint test returned with status {process.returncode}")
    return process.returncode == 0


async def get_zookeeper_quorum(ops_test: OpsTest, zookeeper_name: str) -> str:
    addresses = []
    for unit in ops_test.model.applications[zookeeper_name].units:
        host = await get_address(ops_test, unit.name)
        port = ZOOKEEPER_PORT
        addresses.append(f"{host}:{port}")
    return ",".join(addresses)


async def get_active_kyuubi_servers_list(
    ops_test: OpsTest, zookeeper_name=ZOOKEEPER_NAME
) -> list[str]:
    """Return the list of Kyuubi servers that are live in the cluster."""
    zookeeper_quorum = await get_zookeeper_quorum(ops_test=ops_test, zookeeper_name=zookeeper_name)
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
        "1.9.0",
    ]
    kubectl_command = [
        "kubectl",
        "exec",
        "kyuubi-k8s-0",
        "-c",
        "kyuubi",
        "-n",
        ops_test.model_name,
        "--",
        *pod_command,
    ]

    process = subprocess.run(kubectl_command, capture_output=True, check=True)
    assert process.returncode == 0

    output_lines = process.stdout.decode().splitlines()
    pattern = r"\?\s+/kyuubi\s+\?\s+(?P<node>[\w\-.]+)\s+\?\s+(?P<port>\d+)\s+\?\s+(?P<version>[\d.]+)\s+\?"
    servers = []

    for line in output_lines:
        match = re.match(pattern, line)
        if not match:
            continue
        servers.append(match.group("node"))

    return servers


async def find_leader_unit(ops_test, app_name):
    """Returns the leader unit of a given application."""
    for unit in ops_test.model.applications[app_name].units:
        if await unit.is_leader_from_status():
            return unit
    return None


async def delete_pod(pod_name, namespace):
    """Delete a pod with given name and namespace."""
    command = ["kubectl", "delete", "pod", pod_name, "-n", namespace]
    process = subprocess.run(command, capture_output=True, check=True)
    assert process.returncode == 0, f"Could not delete the pod {pod_name}."


async def get_kyuubi_pid(ops_test: OpsTest, unit):
    """Return the process ID of Kyuubi process in given pod."""
    pod_name = unit.name.replace("/", "-")
    command = [
        "kubectl",
        "exec",
        pod_name,
        "-c",
        KYUUBI_CONTAINER_NAME,
        "-n",
        ops_test.model_name,
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


async def kill_kyuubi_process(ops_test, unit, kyuubi_pid):
    """Kill the Kyuubi process with given PID running in the given unit."""
    pod_name = unit.name.replace("/", "-")
    command = [
        "kubectl",
        "exec",
        pod_name,
        "-c",
        KYUUBI_CONTAINER_NAME,
        "-n",
        ops_test.model_name,
        "--",
        "kill",
        "-SIGKILL",
        kyuubi_pid,
    ]
    process = subprocess.run(command, capture_output=True, check=True)
    assert process.returncode == 0, f"Could not kill Kyuubi process with pid {kyuubi_pid}."


async def is_entire_cluster_responding_requests(ops_test: OpsTest, test_pod) -> bool:
    """Return whether the entire Kyuubi cluster is responding to requests from client."""
    jdbc_endpoint = await fetch_jdbc_endpoint(ops_test)

    kyuubi_pods = {
        unit.name.replace("/", "-") for unit in ops_test.model.applications[APP_NAME].units
    }
    logger.info(f"Nodes in the cluster being tested: {','.join(kyuubi_pods)}")
    pods_that_responded = set()

    tries = 0
    max_tries = 20
    command_executed_at = None

    while True:
        logger.info(f"Trying the {tries + 1}-th connection to see if entire cluster responds...")
        unique_id = get_random_name()
        query = f"SELECT '{unique_id}'"
        pod_command = ["/opt/kyuubi/bin/beeline", "-u", jdbc_endpoint, "-e", query]
        kubectl_command = [
            "kubectl",
            "exec",
            test_pod,
            "-n",
            ops_test.model_name,
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
                ops_test.model_name,
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


async def juju_sleep(ops: OpsTest, time: int, app: str | None = None):
    """Sleep for given amount of time while waiting for the given application to be idle."""
    app_name = app if app else ops.model.applications[0]

    await ops.model.wait_for_idle(
        apps=[app_name],
        idle_period=time,
        timeout=300,
    )


def prometheus_exporter_data(host: str) -> str | None:
    """Check if a given host has metric service available and it is publishing."""
    url = f"http://{host}:{COS_METRICS_PORT}/metrics"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        return
    if response.status_code == 200:
        return response.text


async def all_prometheus_exporters_data(ops_test: OpsTest, check_field) -> bool:
    """Check if a all units has metric service available and publishing."""
    result = True
    for unit in ops_test.model.applications[APP_NAME].units:
        unit_ip = await get_address(ops_test, unit.name)
        result = result and check_field in prometheus_exporter_data(unit_ip)
    return result


def published_prometheus_alerts(ops_test: OpsTest, host: str) -> dict | None:
    """Retrieve all Prometheus Alert rules that have been published."""
    if "http://" in host:
        host = host.split("//")[1]
    url = f"http://{host}/{ops_test.model.name}-prometheus-0/api/v1/rules"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        return

    if response.status_code == 200:
        return response.json()


def published_prometheus_data(ops_test: OpsTest, host: str, field: str) -> dict | None:
    """Check the existence of field among Prometheus published data."""
    if "http://" in host:
        host = host.split("//")[1]
    url = f"http://{host}/{ops_test.model.name}-prometheus-0/api/v1/query?query={field}"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        return

    if response.status_code == 200:
        return response.json()


async def published_grafana_dashboards(ops_test: OpsTest) -> str | None:
    """Get the list of dashboards published to Grafana."""
    base_url, pw = await get_grafana_access(ops_test)
    url = f"{base_url}/api/search?query=&starred=false"

    try:
        session = requests.Session()
        session.auth = ("admin", pw)
        response = session.get(url)
    except requests.exceptions.RequestException:
        return
    if response.status_code == 200:
        return response.json()


async def published_loki_logs(
    ops_test: OpsTest, field: str, value: str, limit: int = 300
) -> str | None:
    """Get the list of dashboards published to Grafana."""
    base_url = await get_cos_address(ops_test)
    url = f"{base_url}/{ops_test.model.name}-loki-0/loki/api/v1/query_range"

    try:
        response = requests.get(url, params={"query": f'{{{field}=~"{value}"}}', "limit": limit})
    except requests.exceptions.RequestException:
        return
    if response.status_code == 200:
        return response.json()


async def get_cos_address(ops_test: OpsTest) -> str:
    """Retrieve the URL where COS services are available."""
    cos_addr_res = check_output(
        f"JUJU_MODEL={ops_test.model.name} juju run traefik/0 show-proxied-endpoints --format json",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )

    try:
        cos_addr = json.loads(cos_addr_res)
    except json.JSONDecodeError:
        raise ValueError

    endpoints = cos_addr["traefik/0"]["results"]["proxied-endpoints"]
    return json.loads(endpoints)["traefik"]["url"]


async def get_grafana_access(ops_test: OpsTest) -> tuple[str, str]:
    """Get Grafana URL and password."""
    grafana_res = check_output(
        f"JUJU_MODEL={ops_test.model.name} juju run grafana/0 get-admin-password --format json",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )

    try:
        grafana_data = json.loads(grafana_res)
    except json.JSONDecodeError:
        raise ValueError

    url = grafana_data["grafana/0"]["results"]["url"]
    password = grafana_data["grafana/0"]["results"]["admin-password"]
    return url, password


async def get_address(ops_test: OpsTest, unit_name: str) -> str:
    """Get the address for a unit."""
    status = await ops_test.model.get_status()  # noqa: F821
    app_name = unit_name.split("/")[0]
    address = status["applications"][app_name]["units"][f"{unit_name}"]["address"]
    return address


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
    }
    if not deploy_from_charmhub:
        image_version = METADATA["resources"]["kyuubi-image"]["upstream-source"]
        resources = {"kyuubi-image": image_version}
        logger.info(f"Image version: {image_version}")

        deploy_args.update({"resources": resources})

    # Deploy the Kyuubi charm and wait
    logger.info("Deploying kyuubi-k8s charm...")
    juju.deploy(kyuubi_charm, **deploy_args)
    logger.info("Waiting for kyuubi-k8s app to be settle...")
    status = juju.wait(jubilant.all_blocked)
    logger.info(f"State of kyuubi-k8s app: {status.apps[APP_NAME]}")

    # Set Kyuubi config options and wait
    logger.info("Setting configuration for kyuubi-k8s charm...")
    namespace = juju.model
    username = "kyuubi-spark-engine"
    charm_config = {"namespace": namespace, "service-account": username}

    juju.config(APP_NAME, charm_config)
    logger.info("Waiting for kyuubi-k8s app to settle...")
    status = juju.wait(jubilant.all_blocked)
    assert status.apps[APP_NAME].app_status.message == Status.MISSING_INTEGRATION_HUB.value.message

    # Deploy the S3 Integrator charm and wait
    logger.info("Deploying s3-integrator charm...")
    juju.deploy(**charm_versions.s3.deploy_dict())
    logger.info("Waiting for s3-integrator app to be idle...")
    status = juju.wait(
        lambda status: jubilant.all_blocked(status, charm_versions.s3.app),
    )

    # Receive S3 params from fixture, apply them and wait
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

    # Deploy the integration hub charm and wait
    logger.info("Deploying integration-hub charm...")
    juju.deploy(**charm_versions.integration_hub.deploy_dict())
    logger.info("Waiting for integration_hub and s3-integrator app to be idle and active...")
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.s3.app,
            charm_versions.integration_hub.app,
        )
    )

    # Integrate integration hub with S3 integrator and wait
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

    # Integrate Kyuubi with Integration Hub and wait
    logger.info("Integrating kyuubi charm with integration-hub charm...")
    juju.integrate(charm_versions.integration_hub.app, APP_NAME)
    logger.info(
        "Waiting for kyuubi, s3-integrator and integration_hub charms to be idle and active..."
    )
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.s3.app,
            charm_versions.integration_hub.app,
        ),
        delay=5,
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

    logger.info("Successfully deployed minimal working Kyuubi setup.")


def get_k8s_service(namespace: str, service_name: str):
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


async def run_command_in_pod(
    ops_test: OpsTest,
    pod_name: str,
    pod_command: List[str],
) -> tuple[str, str]:
    """Load certificate in the pod."""
    kubectl_command = [
        "kubectl",
        "exec",
        pod_name,
        "-c",
        "kyuubi",
        "-n",
        ops_test.model_name,
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
    namespace,
    service_type,
):
    """Utility function to check status of managed K8s service created by Kyuubi charm."""
    service_name = f"{APP_NAME}-service"
    service = get_k8s_service(namespace=namespace, service_name=service_name)
    logger.info(f"{service=}")

    assert service is not None

    service_spec = service.spec
    assert service_type == service_spec.type
    assert service_spec.selector == {"app.kubernetes.io/name": APP_NAME}

    service_port = service_spec.ports[0]
    assert service_port.port == JDBC_PORT
    assert service_port.targetPort == JDBC_PORT
    assert service_port.name == JDBC_PORT_NAME
    assert service_port.protocol == "TCP"

    if service_type in ("NodePort", "LoadBalancer"):
        assert NODEPORT_MIN_VALUE <= service_port.nodePort <= NODEPORT_MAX_VALUE


async def fetch_spark_properties(ops_test: OpsTest, unit_name: str) -> dict[str, str]:
    pod_name = unit_name.replace("/", "-")
    command = ["cat", "/etc/spark8t/conf/spark-defaults.conf"]
    stdout, stderr = await run_command_in_pod(
        ops_test=ops_test, pod_name=pod_name, pod_command=command
    )
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
):
    """Run simple SQL queries to validate Kyuubi and return whether this validation is successful."""
    if not kyuubi_host:
        kyuubi_host = juju.status().apps[APP_NAME].units[f"{APP_NAME}/0"].address
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
    kyuubi_client = KyuubiClient(**args)

    with kyuubi_client.connection as conn, conn.cursor() as cursor:
        for line in query_lines:
            cursor.execute(line)
        results = cursor.fetchall()
        return len(results) == 1
