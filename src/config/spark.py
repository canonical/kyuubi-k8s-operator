#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Spark related configurations."""

from typing import Optional

from lightkube import Client

from constants import KYUUBI_OCI_IMAGE
from core.config import CharmConfig
from core.domain import SparkServiceAccountInfo
from utils.logging import WithLogging


class SparkConfig(WithLogging):
    """Spark Configurations."""

    def __init__(
        self,
        charm_config: CharmConfig,
        service_account_info: Optional[SparkServiceAccountInfo],
    ):
        self.charm_config = charm_config
        self.service_account_info = service_account_info

    def _get_spark_master(self) -> str:
        cluster_address = Client().config.cluster.server
        return f"k8s://{cluster_address}"

    def _base_conf(self):
        """Return base Spark configurations."""
        conf = {
            "spark.master": self._get_spark_master(),
            "spark.kubernetes.container.image": KYUUBI_OCI_IMAGE,
            "spark.submit.deployMode": "cluster",
        }
        if self.charm_config.enable_dynamic_allocation:
            conf.update(
                {
                    "spark.dynamicAllocation.enabled": "true",
                    "spark.dynamicAllocation.shuffleTracking.enabled": "true",
                }
            )
        return conf

    def _sa_conf(self):
        """Spark configurations read from Spark8t."""
        if not self.service_account_info:
            return {}
        namespace, username = self.service_account_info.service_account.split(":")
        conf = {
            "spark.kubernetes.namespace": namespace,
            "spark.kubernetes.authenticate.driver.serviceAccountName": username,
        }
        conf.update(self.service_account_info.spark_properties)
        return conf

    def to_dict(self) -> dict[str, str]:
        """Return the dict representation of the configuration file.

        The configurations are merged with the following order of priority (1 is highest):
            1. Configurations associated with service account read from Spark8t
            2. Base configurations
        """
        return self._base_conf() | self._sa_conf()

    @property
    def contents(self) -> str:
        """Return configuration contents formatted to be consumed by pebble layer."""
        dict_content = self.to_dict()

        return "\n".join(
            [
                f"{key}={value}"
                for key in sorted(dict_content.keys())
                if (value := dict_content[key])
            ]
        )
