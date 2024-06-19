#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Spark related configurations."""

from typing import Optional

from lightkube import Client
from spark8t.services import K8sServiceAccountRegistry, LightKube

from constants import KYUUBI_OCI_IMAGE
from core.domain import S3ConnectionInfo, SparkServiceAccountInfo
from utils.logging import WithLogging


class SparkConfig(WithLogging):
    """Spark Configurations."""

    def __init__(
        self,
        s3_info: Optional[S3ConnectionInfo],
        service_account_info: Optional[SparkServiceAccountInfo],
    ):
        self.s3_info = s3_info
        self.service_account_info = service_account_info

    def _get_upload_path(self) -> str:
        bucket_name = self.s3_info.bucket or "kyuubi"
        return f"s3a://{bucket_name}/"

    def _get_sql_warehouse_path(self) -> str:
        bucket_name = self.s3_info.bucket or "kyuubi"
        warehouse_dir = "warehouse"
        return f"s3a://{bucket_name}/{warehouse_dir}"

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

        if service_account := registry.get(account_id):
            return service_account.configurations.props

        self.logger.warning(f"Account {account_id} does not exist")

        return {}

    def _user_conf(self):
        """Spark configurations generated from relations."""
        conf = {}
        if self.s3_info:
            conf.update(
                {
                    "spark.hadoop.fs.s3a.endpoint": self.s3_info.endpoint,
                    "spark.hadoop.fs.s3a.access.key": self.s3_info.access_key,
                    "spark.hadoop.fs.s3a.secret.key": self.s3_info.secret_key,
                    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                    "spark.hadoop.fs.s3a.path.style.access": "true",
                    "spark.sql.warehouse.dir": self._get_sql_warehouse_path(),
                    "spark.kubernetes.file.upload.path": self._get_upload_path(),
                }
            )
        return conf

    def to_dict(self) -> dict[str, str]:
        """Return the dict representation of the configuration file.

        The configurations are merged with the following order of priority (1 is highest):
            1. User relation configurations
            2. Configurations associated with service account read from Spark8t
            3. Base configurations
        """
        return self._base_conf() | self._sa_conf() | self._user_conf()

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
