#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Integration Hub manager."""

import base64
import re

import yaml

from constants import TRUSTSTORE_SECRET_PREFIX
from core.domain import IntegrationHubTrustStore, SparkServiceAccountInfo
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

    def get_hub_truststore(self) -> IntegrationHubTrustStore | None:
        """Extract truststore content from the integration hub resource manifest, if available."""
        if not self.resource_manifest:
            self.logger.debug("No resource manifest provided for extracting truststore")
            return None
        try:
            manifests = yaml.safe_load_all(self.resource_manifest)
        except yaml.YAMLError:
            self.logger.warning(
                "Invalid resource-manifest YAML from integration hub, cannot extract truststore."
            )
            return None

        truststore_secret = next(
            (
                m
                for m in manifests
                if isinstance(m, dict)
                and m.get("kind") == "Secret"
                and isinstance(m.get("metadata"), dict)
                and isinstance(m["metadata"].get("name"), str)
                and m["metadata"]["name"].startswith(TRUSTSTORE_SECRET_PREFIX)
            ),
            None,
        )
        if not truststore_secret:
            self.logger.debug("No truststore secret found in resource manifest.")
            return None

        secret_data = truststore_secret.get("data")
        if not isinstance(secret_data, dict) or not secret_data:
            self.logger.warning(
                "Truststore secret found in resource-manifest but has no data",
            )
            return None

        truststore_filename = next(iter(secret_data.keys()), None)
        if not isinstance(truststore_filename, str):
            self.logger.warning(
                "Truststore secret found in resource-manifest but has no valid filename",
            )
            return None

        truststore_b64 = next(iter(secret_data.values()), None)
        if not isinstance(truststore_b64, str):
            self.logger.warning(
                "Truststore secret found in resource-manifest but has no valid base64 data",
            )
            return None

        truststore_content = base64.b64decode(truststore_b64, validate=True)
        return IntegrationHubTrustStore(
            secret_name=truststore_secret["metadata"]["name"],
            file_name=truststore_filename,
            content=truststore_content,
        )
