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

    def _compare_and_update_file(self, content: str, file_path: str) -> bool:
        """Update the file at given file_path with given content.

        Before doing the update, compare the existing content of the file and update
        it only if has changed.

        Return True if the file was re-written, else False.
        """
        try:
            existing_content = self.workload.read(file_path)
        except FileNotFoundError:
            existing_content = ""
        self.logger.debug(f"{file_path=}")
        self.logger.debug(f"{existing_content=}")
        self.logger.debug(f"{content=}")
        if existing_content != content:
            self.workload.write(content, file_path)
            return True

        return False

    def update(
        self,
        s3_info: S3ConnectionInfo | None,
        metastore_db_info: DatabaseConnectionInfo | None,
        auth_db_info: DatabaseConnectionInfo | None,
        service_account_info: SparkServiceAccountInfo | None,
        zookeeper_info: ZookeeperInfo | None,
    ):
        """Update Kyuubi service and restart it."""
        # Restart workload only if some configuration has changed.
        if any(
            [
                self._compare_and_update_file(
                    SparkConfig(
                        s3_info=s3_info, service_account_info=service_account_info
                    ).contents,
                    self.workload.SPARK_PROPERTIES_FILE,
                ),
                self._compare_and_update_file(
                    HiveConfig(db_info=metastore_db_info).contents,
                    self.workload.HIVE_CONFIGURATION_FILE,
                ),
                self._compare_and_update_file(
                    KyuubiConfig(db_info=auth_db_info, zookeeper_info=zookeeper_info).contents,
                    self.workload.KYUUBI_CONFIGURATION_FILE,
                ),
            ]
        ):
            self.workload.restart()
        else:
            self.logger.info(
                "Workload restart skipped because the configurations have not changed."
            )
