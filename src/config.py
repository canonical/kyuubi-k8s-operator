#!/usr/bin/env python3

"""Spark History Server configuration."""

import re
from typing import Optional
from s3 import S3ConnectionInfo
from lightkube import Client

from utils import WithLogging

KYUUBI_OCI_IMAGE = "ghcr.io/canonical/charmed-spark:3.4-22.04_edge"

class KyuubiServerConfig(WithLogging):
    """Spark History Server Configuration."""          


    def __init__(self, s3_info: Optional[S3ConnectionInfo], namespace: str, service_account: str):
        self.s3_info = s3_info
        self.namespace = namespace
        self.service_account = service_account

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

    @property
    def _base_conf(self) -> dict[str, str]:
        return {
            "spark.sql.warehouse.dir": self._get_sql_warehouse_path(),
        }

    @property
    def _k8s_conf(self) -> dict[str, str]:
        return {
            "spark.master": self._get_spark_master(),
            "spark.kubernetes.container.image": KYUUBI_OCI_IMAGE,
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.kubernetes.namespace": "spark",
            "spark.submit.deployMode": "cluster",
            "spark.kubernetes.file.upload.path": self._get_upload_path(),
        }
    
    @property
    def _s3_conf(self) -> dict[str, str]:
        conf = {
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.path.style.access": "true",
        }        
        if self.s3_info:
            conf.update({
                "spark.hadoop.fs.s3a.endpoint": self.s3_info.endpoint,
                "spark.hadoop.fs.s3a.access.key": self.s3_info.access_key,
                "spark.hadoop.fs.s3a.secret.key": self.s3_info.secret_key,
            })
        return conf

    def to_dict(self) -> dict[str, str]:
        """Return the dict representation of the configuration file."""
        return self._base_conf | self._k8s_conf | self._s3_conf

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
