"""Utility methods related to Kubernetes resources."""

from lightkube import Client
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import Namespace, ServiceAccount


def is_valid_namespace(namespace: str) -> bool:
    """Return whether given namespace exists in K8s cluster."""
    try:
        Client().get(Namespace, name=namespace)
    except ApiError:
        return False
    return True


def is_valid_service_account(namespace: str, service_account: str) -> bool:
    """Return whether given service account in the given namespace exists in K8s cluster."""
    try:
        Client().get(ServiceAccount, name=service_account, namespace=namespace)
    except ApiError:
        return False
    return True
