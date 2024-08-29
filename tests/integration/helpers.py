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
