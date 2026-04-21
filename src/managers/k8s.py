#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""K8s manager."""

from lightkube import Client
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import Namespace, Node, ServiceAccount

from core.domain import SparkServiceAccountInfo
from utils.logging import WithLogging


class K8sManager(WithLogging):
    """Class that encapsulates various utilities related to K8s."""

    def __init__(self, service_account_info: SparkServiceAccountInfo):
        self.namespace, self.service_account = service_account_info.service_account.split(":")

    def is_namespace_valid(self):
        """Return whether given namespace exists in K8s cluster."""
        try:
            Client().get(Namespace, name=self.namespace)
        except ApiError:
            return False
        return True

    def is_service_account_valid(self):
        """Return whether given service account in the given namespace exists in K8s cluster."""
        try:
            Client().get(ServiceAccount, name=self.service_account, namespace=self.namespace)
        except ApiError:
            return False
        return True

    def verify(self) -> bool:
        """Verify service account information."""
        return self.is_namespace_valid() and self.is_service_account_valid()

    def get_number_of_gpus(self) -> int:
        """Get the total number of GPUs across all nodes."""
        n_gpus = 0
        for node in Client().list(Node):
            n_gpus += int(getattr(node.status, "capacity", {}).get("nvidia.com/gpu", 0))
        return n_gpus
