# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import lightkube
import yaml
from canonical_service_mesh.k8s.types.istio import AuthorizationPolicy

from core.domain import PeerAuthentication

from ..types import IntegrationTestsCharms

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
JDBC_PORT = 10009


def deploy_istio_mesh_setup(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
) -> None:
    """Deploy the Istio mesh setup."""
    logger.info("Deploying Istio K8s charm")
    juju.deploy(**charm_versions.istio.deploy_dict())
    juju.wait(
        lambda status: jubilant.all_active(status, charm_versions.istio.application_name), delay=5
    )

    logger.info("Deploying Istio beacon charm")
    juju.deploy(**charm_versions.istio_beacon.deploy_dict())
    juju.wait(
        lambda status: jubilant.all_active(status, charm_versions.istio_beacon.application_name),
        delay=5,
    )

    logger.info("Integrating Kyuubi charm with istio beacon charm")
    juju.integrate(
        f"{APP_NAME}:service-mesh", f"{charm_versions.istio_beacon.application_name}:service-mesh"
    )
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=5)


def has_kyuubi_jdbc_authorization_policy(namespace: str, app_name: str) -> bool:
    """Check if an AuthorizationPolicy in `namespace` allows connections to the JDBC port."""
    client = lightkube.Client()
    result = client.list(
        AuthorizationPolicy,
        namespace=namespace,
        labels={
            "app.kubernetes.io/instance": f"{app_name}-{namespace}",
            "kubernetes-resource-handler-scope": f"{app_name}-jdbc-policy",
        },
    )
    if result is None:
        return False
    policies = list(result)
    if len(policies) == 0:
        logger.info(f"No AuthorizationPolicy found for app {app_name} in namespace {namespace}")
        return False
    spec = policies[0].get("spec") or {}
    selector = spec.get("selector") or {}
    if selector.get("matchLabels") != {"app.kubernetes.io/name": app_name}:
        logger.error(
            f"AuthorizationPolicy selector does not match expected labels for app {app_name}. Spec: {spec}"
        )
        return False
    rules = spec.get("rules") or []
    for rule in rules:
        for to in rule.get("to") or []:
            ports = (to.get("operation") or {}).get("ports") or []
            if str(JDBC_PORT) in ports:
                return True
    return False


def has_kyuubi_jdbc_peer_authentication(namespace: str, app_name: str) -> bool:
    """Check if a PeerAuthentication in `namespace` allows connections to the JDBC port."""
    client = lightkube.Client()
    result = client.list(
        PeerAuthentication,
        namespace=namespace,
        labels={
            "app.kubernetes.io/instance": f"{app_name}-{namespace}",
            "kubernetes-resource-handler-scope": f"{app_name}-jdbc-peer-authentication",
        },
    )
    if result is None:
        return False
    peer_auths = list(result)
    if len(peer_auths) == 0:
        logger.info(f"No PeerAuthentication found for app {app_name} in namespace {namespace}")
        return False
    spec = peer_auths[0].get("spec") or {}
    selector = spec.get("selector") or {}
    if selector.get("matchLabels") != {"app.kubernetes.io/name": app_name}:
        logger.error(
            f"PeerAuthentication selector does not match expected labels for app {app_name}. Spec: {spec}"
        )
        return False
    port_level_mtls = spec.get("portLevelMtls") or {}
    if port_level_mtls.get(str(JDBC_PORT), {}).get("mode") == "PERMISSIVE":
        return True
    return False
