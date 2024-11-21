#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Kyuubi manager."""

from config.hive import HiveConfig
from config.kyuubi import KyuubiConfig
from config.spark import SparkConfig
from core.domain import (
    DatabaseConnectionInfo,
    S3ConnectionInfo,
    SparkServiceAccountInfo,
    ZookeeperInfo,
)
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
        zookeeper_info: ZookeeperInfo | None,
    ):
        """Update Kyuubi service and restart it."""
        restart_required = False

        new_spark_config = SparkConfig(
            s3_info=s3_info, service_account_info=service_account_info
        ).contents
        try:
            existing_spark_config = self.workload.read(self.workload.SPARK_PROPERTIES_FILE)
        except FileNotFoundError:
            existing_spark_config = ""
        if existing_spark_config != new_spark_config:
            self.workload.write(new_spark_config, self.workload.SPARK_PROPERTIES_FILE)
            restart_required = True

        new_hive_config = HiveConfig(db_info=metastore_db_info).contents
        try:
            existing_hive_config = self.workload.read(self.workload.HIVE_CONFIGURATION_FILE)
        except FileNotFoundError:
            existing_hive_config = ""
        if existing_hive_config != new_hive_config:
            self.workload.write(new_hive_config, self.workload.HIVE_CONFIGURATION_FILE)
            restart_required = True

        new_kyuubi_config = KyuubiConfig(
            db_info=auth_db_info, zookeeper_info=zookeeper_info
        ).contents
        try:
            existing_kyuubi_config = self.workload.read(self.workload.KYUUBI_CONFIGURATION_FILE)
        except FileNotFoundError:
            existing_kyuubi_config = ""
        if existing_kyuubi_config != new_kyuubi_config:
            self.workload.write(new_kyuubi_config, self.workload.KYUUBI_CONFIGURATION_FILE)
            restart_required = True

        if not restart_required:
            self.logger.info(
                "Workload restart skipped because the configurations have not changed."
            )
            return

        self.workload.restart()
