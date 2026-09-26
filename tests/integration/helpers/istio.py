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

# Label the integration hub stamps on the Kubernetes resources it manages.
MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
MANAGED_BY_INTEGRATION_HUB = "integration-hub"


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


def _spiffe_principal(workload_namespace: str, workload_service_account: str) -> str:
    """Build the SPIFFE principal for a given service account."""
    return f"cluster.local/ns/{workload_namespace}/sa/{workload_service_account}"


def _is_managed_by_integration_hub(policy) -> bool:
    labels = (policy.metadata.labels or {}) if policy.metadata else {}
    return labels.get(MANAGED_BY_LABEL) == MANAGED_BY_INTEGRATION_HUB


def _policy_selector_labels(policy) -> dict[str, str]:
    return (policy.spec or {}).get("selector", {}).get("matchLabels", {})


def _policy_principals(policy) -> set[str]:
    principals: set[str] = set()
    for rule in (policy.spec or {}).get("rules", []):
        for source_rule in rule.get("from", []):
            principals.update(source_rule.get("source", {}).get("principals", []))
    return principals


def _workload_auth_policy_exists(workload_namespace: str, role: str, principal: str) -> bool:
    """Check for an ALLOW policy selecting `spark-role=role` that allows the workload SA.

    Matches on behaviour (managed-by label, selector, action and allowed
    principal) rather than the generated policy name.
    """
    client = lightkube.Client()
    for policy in client.list(AuthorizationPolicy, namespace=workload_namespace):
        spec = policy.spec or {}
        if not _is_managed_by_integration_hub(policy):
            continue
        if spec.get("action") != "ALLOW":
            continue
        if _policy_selector_labels(policy).get("spark-role") != role:
            continue
        if principal in _policy_principals(policy):
            return True
    return False


def has_authorization_policy_to_spark_driver(
    workload_namespace: str,
    workload_service_account: str,
    kyuubi_namespace: str,
    kyuubi_service_account: str,
) -> bool:
    """Whether an authorization policy grants access to the driver of the workload SA."""
    workload_principal = _spiffe_principal(workload_namespace, workload_service_account)
    workload_allowed = _workload_auth_policy_exists(
        workload_namespace=workload_namespace, role="driver", principal=workload_principal
    )
    kyuubi_principal = _spiffe_principal(kyuubi_namespace, kyuubi_service_account)
    kyuubi_allowed = _workload_auth_policy_exists(
        workload_namespace=kyuubi_namespace, role="kyuubi", principal=kyuubi_principal
    )
    return workload_allowed and kyuubi_allowed


def has_authorization_policy_to_spark_executor(
    workload_namespace: str, workload_service_account: str
) -> bool:
    """Whether an authorization policy grants access to the executors of the workload SA."""
    workload_principal = _spiffe_principal(workload_namespace, workload_service_account)
    return _workload_auth_policy_exists(
        workload_namespace=workload_namespace, role="executor", principal=workload_principal
    )


def has_authorization_policy_from_driver_to_kyuubi(
    workload_namespace: str,
    workload_service_account: str,
    kyuubi_namespace: str,
    kyuubi_service_account: str,
) -> bool:
    """Whether a policy in the kyuubi namespace allows the workload SA to reach it.

    Matches on behaviour: an ALLOW policy in `kyuubi_namespace` selecting the
    Kyuubi pods and allowing the workload service account principal.
    """
    client = lightkube.Client()
    principal = _spiffe_principal(workload_namespace, workload_service_account)
    for policy in client.list(AuthorizationPolicy, namespace=kyuubi_namespace):
        spec = policy.spec or {}
        if not _is_managed_by_integration_hub(policy):
            continue
        if spec.get("action") != "ALLOW":
            continue
        if _policy_selector_labels(policy).get("app.kubernetes.io/name") != kyuubi_service_account:
            continue
        if principal in _policy_principals(policy):
            return True
    return False
