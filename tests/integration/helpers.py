import datetime
import logging
import re
import subprocess
import uuid
from pathlib import Path

import yaml
from pytest_operator.plugin import OpsTest

from constants import (
    HA_ZNODE_NAME,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
ZOOKEEPER_NAME = "zookeeper-k8s"
TEST_CHARM_PATH = "./tests/integration/app-charm"
TEST_CHARM_NAME = "application"

PROCESS_NAME_PATTERN = "org.apache.kyuubi.server.KyuubiServer"
KYUUBI_CONTAINER_NAME = "kyuubi"


def get_random_name():
    return str(uuid.uuid4()).replace("-", "_")


async def fetch_jdbc_endpoint(ops_test):
    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    kyuubi_unit = ops_test.model.applications[APP_NAME].units[0]
    action = await kyuubi_unit.run_action(
        action_name="get-jdbc-endpoint",
    )
    result = await action.wait()

    jdbc_endpoint = result.results.get("endpoint")
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    return jdbc_endpoint


async def run_sql_test_against_jdbc_endpoint(ops_test: OpsTest, test_pod):
    """Verify the JDBC endpoint exposed by the charm with some SQL queries."""
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


# async def get_kyuubi_znode_children(ops_test: OpsTest, test_pod) -> list[str]:
#     """Return the list of znodes created by Kyuubi when Kyuubi units register with zookeeper"""

#     logger.info("Running action 'get-super-password' on zookeeper-k8s unit...")
#     zookeeper_unit = ops_test.model.applications[ZOOKEEPER_NAME].units[0]
#     action = await zookeeper_unit.run_action(
#         action_name="get-super-password",
#     )
#     result = await action.wait()

#     password = result.results.get("super-password")
#     logger.info(f"super user password: {password}")

#     status = await ops_test.model.get_status()
#     zookeeper_address = status["applications"][ZOOKEEPER_NAME]["units"][f"{ZOOKEEPER_NAME}/0"]["address"]

#     zk = KazooClient(
#         hosts=f"{zookeeper_address}:2181",
#         timeout=1.0,
#         sasl_options={"mechanism": "DIGEST-MD5", "username": "super", "password": password},
#         verify_certs=False,
#         use_ssl = False
#     )
#     zk.start()
#     znodes = zk.get_children(HA_ZNODE_NAME)
#     zk.stop()
#     return znodes


async def get_active_kyuubi_servers_list(ops_test: OpsTest) -> list[str]:
    """Return the list of Kyuubi servers that are live in the cluster."""
    jdbc_endpoint = await fetch_jdbc_endpoint(ops_test)
    zookeper_quorum = jdbc_endpoint.split(";")[0].split("//")[-1]

    pod_command = [
        "/opt/kyuubi/bin/kyuubi-ctl",
        "list",
        "server",
        "--zk-quorum",
        zookeper_quorum,
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
    for unit in ops_test.model.applications[app_name].units:
        if await unit.is_leader_from_status():
            return unit
    return None


async def delete_pod(pod_name, namespace):
    command = ["kubectl", "delete", "pod", pod_name, "-n", namespace]
    process = subprocess.run(command, capture_output=True, check=True)
    assert process.returncode == 0, f"Could not delete the pod {pod_name}."


async def get_kyuubi_pid(ops_test: OpsTest, unit):
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
    assert (
        process.returncode == 0
    ), f"Command: {command} returned with return code {process.returncode}"

    for line in process.stdout.decode().splitlines():
        match = re.search(re.escape(PROCESS_NAME_PATTERN), line)
        if match:
            pid = line.split()[1]
            logger.info(f"Found Kyuubi process with PID: {pid}")
            return pid
    return None


async def kill_kyuubi_process(ops_test, unit, kyuubi_pid):
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
    jdbc_endpoint = await fetch_jdbc_endpoint(ops_test)

    kyuubi_pods = {
        unit.name.replace("/", "-") for unit in ops_test.model.applications[APP_NAME].units
    }
    logger.info(f"Nodes in the cluster being tested: " f"{','.join(kyuubi_pods)}")
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
        logger.info(
            f"Executing command: {' '.join(kubectl_command)} " f"at {command_executed_at}..."
        )
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
    app_name = app if app else ops.model.applications[0]

    await ops.model.wait_for_idle(
        apps=[app_name],
        idle_period=time,
        timeout=300,
    )
