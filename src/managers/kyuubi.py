#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Kyuubi manager."""

from config.hive import HiveConfig
from config.kyuubi import KyuubiConfig
from config.spark import SparkConfig
from core.domain import DatabaseConnectionInfo, S3ConnectionInfo, SparkServiceAccountInfo
from core.workload import KyuubiWorkloadBase
from utils.logging import WithLogging


class KyuubiManager(WithLogging):
    """Kyuubi manager class."""

    def __init__(self, workload: KyuubiWorkloadBase):
        self.workload = workload

    def update(
        self,
        s3_info: S3ConnectionInfo | None,
        metastore_db_info: DatabaseConnectionInfo | None,
        auth_db_info: DatabaseConnectionInfo | None,
        service_account_info: SparkServiceAccountInfo | None,
    ):
        """Update Kyuubi service and restart it."""
        spark_config = SparkConfig(
            s3_info=s3_info, service_account_info=service_account_info
        ).contents
        hive_config = HiveConfig(db_info=metastore_db_info).contents
        kyuubi_config = KyuubiConfig(db_info=auth_db_info).contents

        self.workload.write(spark_config, self.workload.SPARK_PROPERTIES_FILE)
        self.workload.write(hive_config, self.workload.HIVE_CONFIGURATION_FILE)
        self.workload.write(kyuubi_config, self.workload.KYUUBI_CONFIGURATION_FILE)

        self.workload.restart()
