#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Kyuubi manager."""

from utils.logging import WithLogging
from utils.io import IOMode
from workload.base import KyuubiWorkloadBase
from core.domain import S3ConnectionInfo, DatabaseConnectionInfo, ServiceAccountInfo
from config.spark import SparkConfig
from config.hive import HiveConfig
from config.kyuubi import KyuubiConfig

class KyuubiManager(WithLogging):

    def __init__(self, workload: KyuubiWorkloadBase):
        self.workload = workload
    
    def update(
        self,
        s3_info: S3ConnectionInfo | None = None,
        metastore_db_info: DatabaseConnectionInfo | None = None,
        auth_db_info: DatabaseConnectionInfo | None = None,
        service_account_info: ServiceAccountInfo | None = None,
    ):
        with self.workload.get_spark_configuration_file(IOMode.WRITE) as spark_fid:
            config = SparkConfig(
                s3_info=s3_info, service_account_info=service_account_info
            )
            spark_fid.write(config.contents)
        with self.workload.get_hive_configuration_file(IOMode.WRITE) as hive_fid:
            config = HiveConfig(db_info=metastore_db_info)
            hive_fid.write(config.contents)
        with self.workload.get_kyuubi_configuration_file(IOMode.WRITE) as kyuubi_fid:
            config = KyuubiConfig(db_info=auth_db_info)
            kyuubi_fid.write(config.contents)
        