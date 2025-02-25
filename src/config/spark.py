#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Spark related configurations."""

from typing import Optional

from lightkube import Client
from lightkube.core.exceptions import ApiError
from spark8t.services import K8sServiceAccountRegistry, LightKube

from constants import ICEBERG_CATALOG_NAME, KYUUBI_OCI_IMAGE
from core.config import CharmConfig
from core.domain import DatabaseConnectionInfo, SparkServiceAccountInfo
from utils.logging import WithLogging


class SparkConfig(WithLogging):
    """Spark Configurations."""

    def __init__(
        self,
        charm_config: CharmConfig,
        service_account_info: Optional[SparkServiceAccountInfo],
        metastore_db_info: Optional[DatabaseConnectionInfo],
    ):
        self.charm_config = charm_config
        self.service_account_info = service_account_info
        self.metastore_db_info = metastore_db_info

    def _get_spark_master(self) -> str:
        cluster_address = Client().config.cluster.server
        return f"k8s://{cluster_address}"

    def _base_conf(self):
        """Return base Spark configurations."""
        return {
            "spark.master": self._get_spark_master(),
            "spark.kubernetes.container.image": KYUUBI_OCI_IMAGE,
            "spark.submit.deployMode": "cluster",
        }

    def _sa_conf(self):
        """Spark configurations read from Spark8t."""
        if not self.service_account_info:
            return {}

        interface = LightKube(None, None)
        registry = K8sServiceAccountRegistry(interface)

        account_id = ":".join(
            [self.service_account_info.namespace, self.service_account_info.service_account]
        )

        try:
            service_account = registry.get(account_id)
            return service_account.configurations.props
        except (ApiError, AttributeError):
            self.logger.warning(f"Could not fetch Spark properties from {account_id}.")

        return {}

    def _iceberg_conf(self):
        """ "Apache iceberg related configurations."""
        if not self.charm_config.enable_iceberg:
            return {}

        return {
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.defaultCatalog": ICEBERG_CATALOG_NAME,
            f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.warehouse": "s3a://spark/warehouse",
            f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.type": "hive"
            if self.metastore_db_info
            else "hadoop",
        }

    def to_dict(self) -> dict[str, str]:
        """Return the dict representation of the configuration file.

        The configurations are merged with the following order of priority (1 is highest):
            1. Configurations associated with service account read from Spark8t
            2. Base configurations
        """
        return self._base_conf() | self._sa_conf() | self._iceberg_conf()

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
