# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from __future__ import annotations

import datetime
import logging
import re
import subprocess
import uuid
from pathlib import Path
from typing import cast

import jubilant
import yaml

from constants import HA_ZNODE_NAME

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
ZOOKEEPER_NAME = "zookeeper-k8s"
ZOOKEEPER_PORT = 2181

PROCESS_NAME_PATTERN = "org.apache.kyuubi.server.KyuubiServer"
KYUUBI_CONTAINER_NAME = "kyuubi"


def get_random_name():
    return str(uuid.uuid4()).replace("-", "_")


def delete_pod(pod_name: str, namespace: str) -> None:
    """Delete a pod with given name and namespace."""
    command = ["kubectl", "delete", "pod", pod_name, "-n", namespace]
    process = subprocess.run(command, capture_output=True, check=True)
    assert process.returncode == 0, f"Could not delete the pod {pod_name}."


def delete_engines_pod(namespace: str, pod_prefix: str = "kyuubi-user-spark-sql") -> None:
    """Delete engine pods with given name prefix and namespace."""
    logger.info("Deleting engines pod that are still active.")

    command = ["kubectl", "get", "pods", "-n", namespace]

    process = subprocess.run(command, capture_output=True, check=True)
    assert process.returncode == 0

    output_lines = process.stdout.decode().splitlines()
    for line in output_lines:
        pod_name = line.split()[0]
        if pod_name.startswith(pod_prefix):
            delete_command = ["kubectl", "delete", "pod", pod_name, "-n", namespace]
            try:
                process = subprocess.run(delete_command, capture_output=True, check=True)
                if process.returncode == 0:
                    logger.info(f"Deleted pod: {pod_name}")
            except Exception:
                logger.info(f"Deletion of pod: {pod_name} failed!")


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
