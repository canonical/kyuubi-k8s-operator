#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Kyuubi manager."""

from config.hive import HiveConfig
from config.kyuubi import KyuubiConfig
from config.spark import SparkConfig
from core.context import Context
from core.workload import KyuubiWorkloadBase
from utils.logging import WithLogging


class KyuubiManager(WithLogging):
    """Kyuubi manager class."""

    def __init__(self, workload: KyuubiWorkloadBase, context: Context):
        self.workload = workload
        self.context = context

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
        set_metastore_db_none: bool = False,
        set_auth_db_none: bool = False,
        set_service_account_none: bool = False,
        set_zookeeper_none: bool = False,
        set_tls_none: bool = False,
        force_restart: bool = False,
    ) -> None:
        """Update Kyuubi service and restart it."""
        metastore_db_info = None if set_metastore_db_none else self.context.metastore_db
        auth_db_info = None if set_auth_db_none else self.context.auth_db
        service_account_info = None if set_service_account_none else self.context.service_account
        zookeeper_info = None if set_zookeeper_none else self.context.zookeeper
        tls_info = None if set_tls_none else self.context.tls

        # auth is mandatory
        if not auth_db_info:
            self.logger.info("Workload stopped because auth db is missing.")
            self.workload.stop()
            return

        # Restart workload only if some configuration has changed.
        if any(
            [
                self._compare_and_update_file(
                    SparkConfig(
                        charm_config=self.context.config,
                        service_account_info=service_account_info,
                        metastore_db_info=metastore_db_info,
                    ).contents,
                    self.workload.paths.spark_properties,
                ),
                self._compare_and_update_file(
                    HiveConfig(db_info=metastore_db_info).contents,
                    self.workload.paths.hive_properties,
                ),
                self._compare_and_update_file(
                    KyuubiConfig(
                        db_info=auth_db_info,
                        zookeeper_info=zookeeper_info,
                        tls_info=tls_info,
                        keystore_path=self.workload.paths.keystore,
                    ).contents,
                    self.workload.paths.kyuubi_properties,
                ),
                force_restart,
            ]
        ):
            self.workload.restart()
        else:
            self.logger.info(
                "Workload restart skipped because the configurations have not changed."
            )
