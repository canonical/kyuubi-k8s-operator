#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Utility methods related to Kubernetes resources."""

from lightkube import Client
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import Namespace, ServiceAccount
import subprocess

REQUIRED_PERMISSIONS = {
    "pods": ["create", "get", "list", "watch", "delete"],
    "configmaps": ["create", "get", "list", "watch", "delete"],
    "services": ["create", "get", "list", "watch", "delete"],
}

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
    # for resource, actions in REQUIRED_PERMISSIONS.items():
    #     for action in actions:
    #         sa_identifier = f"system:serviceaccount:{namespace}:{service_account}"
    #         try:
    #             rbac_check = subprocess.run(
    #                 [
    #                     "kubectl",
    #                     "auth",
    #                     "can-i",
    #                     action,
    #                     resource,
    #                     "--namespace",
    #                     namespace,
    #                     "--as",
    #                     sa_identifier,
    #                 ],
    #                 check=True,
    #                 capture_output=True,
    #                 text=True,
    #             )
    #             assert rbac_check.returncode == 0
    #             assert rbac_check.stdout.strip() == "yes"
    #         except:
    #             return False
    return True
