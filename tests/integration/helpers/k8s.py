# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from __future__ import annotations

import logging
import subprocess
import uuid
from pathlib import Path
from typing import Dict, TypedDict, cast

import jubilant
import lightkube
import yaml
from lightkube.core.client import LabelSelector
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import Pod, Service

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

NODEPORT_MIN_VALUE = 30000
NODEPORT_MAX_VALUE = 32767
JDBC_PORT = 10009
JDBC_PORT_NAME = "kyuubi-jdbc"
CURL_IMAGE = "curlimages/curl:8.10.1"


class ContainerSecurityContext(TypedDict, total=False):
    """TypedDict representing Kubernetes container security context settings."""

    runAsUser: int | None  # noqa: N815
    runAsGroup: int | None  # noqa: N815
    runAsNonRoot: bool | None  # noqa: N815


def generate_container_securitycontext_map(
    metadata_yaml: dict, juju_user_id: int = 170
) -> dict[str, ContainerSecurityContext]:
    """Generate a mapping of container names to their expected UID/GID security context."""
    c_uid_map: dict[str, ContainerSecurityContext] = {}
    for name, spec in metadata_yaml.get("containers", {}).items():
        c_uid_map[name] = ContainerSecurityContext(
            runAsUser=spec["uid"],
            runAsGroup=spec["gid"],
        )
    c_uid_map["charm"] = ContainerSecurityContext(
        runAsUser=juju_user_id,
        runAsGroup=juju_user_id,
    )
    return c_uid_map


def assert_security_context(
    lightkube_client: lightkube.Client,
    pod_name: str,
    container_name: str,
    container_securitycontext_map: Dict[str, ContainerSecurityContext],
    model_name: str,
) -> None:
    """Assert that a container's security context matches expected UID/GID settings."""
    pod = lightkube_client.get(Pod, pod_name, namespace=model_name)
    assert pod.spec is not None, f"Pod {pod_name} has no spec"
    containers: list = pod.spec.containers
    container = next((c for c in containers if c.name == container_name), None)
    assert container is not None, f"Container {container_name} not found in pod {pod_name}"
    security_context = container.securityContext
    for key, value in container_securitycontext_map[container_name].items():
        assert getattr(security_context, key) == value


def get_k8s_service(namespace: str, service_name: str) -> Service | None:
    client = lightkube.Client()
    try:
        service = client.get(
            res=Service,
            name=service_name,
            namespace=namespace,
        )
    except ApiError as e:
        if e.status.code == 404:
            return None
        raise

    return service


def run_command_in_pod(
    juju: jubilant.Juju,
    pod_name: str,
    pod_command: list[str],
) -> tuple[str, str]:
    """Run a command in the given pod."""
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


def get_pod_names(model: str, application_name: str) -> list[str]:
    """Retrieve names of all pods belonging to a specific Juju application."""
    cmd = [
        "kubectl",
        "get",
        "pods",
        f"-n{model}",
        f"-lapp.kubernetes.io/name={application_name}",
        "--no-headers",
        "-o=custom-columns=NAME:.metadata.name",
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE)
    return proc.stdout.decode("utf8").split()


def pod_has_labels(
    namespace: str,
    pod_name: str,
    labels: dict[str, str],
) -> bool:
    """Verify and return bool whether the given pod has all the given labels."""
    client = lightkube.Client()
    try:
        pod = client.get(Pod, name=pod_name, namespace=namespace)
        if pod.metadata is None or pod.metadata.labels is None:
            return False
        return all(pod.metadata.labels.get(k) == v for k, v in labels.items())
    except ApiError as e:
        logger.error(f"Failed to get pod {pod_name} in namespace {namespace}: {e}")
        return False


def curl_using_pod(
    namespace: str,
    url: str,
    labels: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a curl command from a temporary pod in the specified namespace."""
    pod_name = f"curl-{uuid.uuid4()}"

    labels_args = []
    if labels:
        # kubectl run --labels accepts a single comma-separated k=v list.
        labels_value = ",".join(f"{key}={value}" for key, value in labels.items())
        labels_args = ["--labels", labels_value]

    return subprocess.run(
        [
            "kubectl",
            "-n",
            namespace,
            "run",
            pod_name,
            "--rm",
            "-i",
            "--quiet",
            "--restart=Never",
            f"--image={CURL_IMAGE}",
            *labels_args,
            "--",
            "curl",
            "-sS",
            "--max-time",
            "10",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            url,
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def get_pods_by_label(labels: dict[str, str], namespace: str | None = None) -> list[str]:
    """Return the names of all pods that carry the given set of labels.

    Args:
        labels: label key/value pairs a pod must all match.
        namespace: namespace to search in. If None, searches all namespaces.
    """
    client = lightkube.Client()
    try:
        pods = client.list(Pod, labels=cast(LabelSelector, labels), namespace=namespace)
        return [pod.metadata.name for pod in pods if pod.metadata and pod.metadata.name]
    except ApiError as e:
        logger.error(f"Error retrieving pods for labels {labels}: {e}")
        return []


def get_pod_ip(pod_name: str, namespace: str) -> str | None:
    """Return the cluster IP address of a pod, or None if not yet assigned.

    Args:
        pod_name: name of the pod.
        namespace: namespace of the pod.
    """
    client = lightkube.Client()
    pod = client.get(Pod, name=pod_name, namespace=namespace)
    return pod.status.podIP if pod.status else None
