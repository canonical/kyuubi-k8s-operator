#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Integration Hub manager."""

import re

from core.domain import SparkServiceAccountInfo
from utils.logging import WithLogging


class IntegrationHubManager(WithLogging):
    """Class that encapsulates various utilities related to K8s."""

    def __init__(self, service_account_info: SparkServiceAccountInfo):
        self.namespace, self.service_account = service_account_info.service_account.split(":")
        self.spark_properties = service_account_info.spark_properties
        self.resource_manifest = service_account_info.resource_manifest

    def is_s3_configured(self) -> bool:
        """Return whether S3 object storage backend has been configured."""
        pattern = r"spark\.hadoop\.fs\.s3a\.secret\.key$"
        return any(re.match(pattern, prop) for prop in self.spark_properties)

    def is_azure_storage_configured(self) -> bool:
        """Return whether Azure object storage backend has been configured."""
        pattern = r"spark\.hadoop\.fs\.azure\.account\.key\..*\.dfs\.core\.windows\.net$"
        return any(re.match(pattern, prop) for prop in self.spark_properties)

    def is_executor_pod_template_configured(self) -> bool:
        """Return whether executor pod template has been configured."""
        pattern = r"spark\.kubernetes\.executor\.podTemplateFile$"
        return any(re.match(pattern, prop) for prop in self.spark_properties)
