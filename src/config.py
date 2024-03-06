#!/usr/bin/env python3
# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

"""Spark History Server configuration."""

import re
from typing import Optional
from s3 import S3ConnectionInfo

from utils import WithLogging


class KyuubiServerConfig(WithLogging):
    """Spark History Server Configuration."""

    _base_conf: dict[str, str] = {
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.path.style.access": "true",
    }


    def __init__(self, s3_info: Optional[S3ConnectionInfo] = None):
        self.s3_info = s3_info

    @property
    def _k8s_conf(self) -> dict[str, str]:
        return {
            "spark.master": "k8s://https://192.168.1.72:16443",
            "spark.kubernetes.container.image": "ghcr.io/canonical/charmed-spark:3.4-22.04_edge",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.kubernetes.namespace": "spark",
            "spark.submit.deployMode": "cluster",
            "spark.kubernetes.file.upload.path": "s3a://kyuubi/",
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
