#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""K8s manager."""

import re

from lightkube import Client
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import Namespace, ServiceAccount
from spark8t.services import K8sServiceAccountRegistry, LightKube

from core.domain import SparkServiceAccountInfo
from core.workload.kyuubi import KyuubiWorkload
from utils.logging import WithLogging


class K8sManager(WithLogging):
    """Class that encapsulates various utilities related to K8s."""

    def __init__(self, service_account_info: SparkServiceAccountInfo, workload: KyuubiWorkload):
        self.namespace = service_account_info.namespace
        self.service_account = service_account_info.service_account
        self.workload = workload

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

    def is_s3_configured(self) -> bool:
        """Return whether S3 object storage backend has been configured."""
        pattern = r"spark\.hadoop\.fs\.s3a\.secret\.key=.*"
        return any(re.match(pattern, prop) for prop in self.workload.get_spark_properties())

    def is_azure_storage_configured(self) -> bool:
        """Return whether Azure object storage backend has been configured."""
        pattern = r"spark\.hadoop\.fs\.azure\.account\.key\..*\.dfs\.core\.windows\.net=.*"
        return any(re.match(pattern, prop) for prop in self.workload.get_spark_properties())

    def has_cluster_permissions(self) -> bool:
        """Return whether the service account has permission to read Spark configurations from the cluster."""
        interface = LightKube(None, None)
        registry = K8sServiceAccountRegistry(interface)
        try:
            registry.get(f"{self.namespace}:{self.service_account}")
        except ApiError:
            return False
        else:
            return True
