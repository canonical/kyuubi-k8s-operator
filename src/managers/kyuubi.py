#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Kyuubi manager."""

from __future__ import annotations

from typing import TYPE_CHECKING

from config.env import KyuubiEnvironConfig
from config.hive import HiveConfig
from config.kyuubi import KyuubiConfig
from config.spark import SparkConfig
from constants import HUB_TRUSTSTORE_MOUNT_BASE, TRUSTSTORE_SECRET_PREFIX
from core.context import Context
from core.workload.kyuubi import KyuubiWorkload
from managers.k8s import K8sManager
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KyuubiCharm
    from core.domain import SparkServiceAccountInfo


class KyuubiManager(WithLogging):
    """Kyuubi manager class."""

    def __init__(
        self,
        charm: KyuubiCharm,
        workload: KyuubiWorkload,
        context: Context,
    ):
        self.charm = charm
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

    def _sync_hub_truststore(self, service_account_info: SparkServiceAccountInfo | None) -> None:
        """Write S3 truststore from integration-hub manifest into the Kyuubi container.

        In client deploy mode, Kyuubi itself runs the Spark driver process, so the truststore
        must exist in the Kyuubi pod filesystem.
        """
        if not service_account_info:
            return

        # Always clean previously managed truststores so stale files do not survive
        # when integration hub removes or rotates truststore secrets.
        stale_prefix = f"{HUB_TRUSTSTORE_MOUNT_BASE}/{TRUSTSTORE_SECRET_PREFIX}"
        if self.workload.exists(HUB_TRUSTSTORE_MOUNT_BASE):
            for path in self.workload.list(HUB_TRUSTSTORE_MOUNT_BASE):
                if path.startswith(stale_prefix):
                    self.workload.delete(path, recursive=True)
                    self.logger.info("Removed stale S3 truststore path %s", path)

        truststore = service_account_info.hub_truststore
        if not truststore:
            self.logger.debug(
                "No truststore found in the integration hub manifest, skipping truststore sync."
            )
            return

        self.workload.write(truststore.content, truststore.path)
        self.logger.info("Synced S3 truststore file at %s", truststore.path)

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

        self._sync_hub_truststore(service_account_info)

        if self.context.config.gpu_enable and self.context.service_account:
            k8s_manager = K8sManager(
                service_account_info=self.context.service_account,
            )
            gpu_capacity = k8s_manager.get_number_of_gpus()
        else:
            gpu_capacity = 0

        # Restart workload only if some configuration has changed.
        should_restart = any(
            [
                self._compare_and_update_file(
                    SparkConfig(
                        charm_config=self.context.config,
                        service_account_info=service_account_info,
                        metastore_db_info=metastore_db_info,
                        gpu_capacity=gpu_capacity,
                    ).contents,
                    self.workload.paths.spark_properties,
                ),
                self._compare_and_update_file(
                    HiveConfig(db_info=metastore_db_info).contents,
                    self.workload.paths.hive_properties,
                ),
                self._compare_and_update_file(
                    KyuubiConfig(
                        charm_config=self.context.config,
                        db_info=auth_db_info,
                        zookeeper_info=zookeeper_info,
                        tls_info=tls_info,
                        keystore_path=(
                            self.workload.paths.keystore
                            if self.workload.exists(self.workload.paths.keystore)
                            else ""
                        ),
                    ).contents,
                    self.workload.paths.kyuubi_properties,
                ),
                self._compare_and_update_file(
                    KyuubiEnvironConfig(service_account_info=service_account_info).contents,
                    self.workload.paths.kyuubi_env,
                ),
                not self.workload.active(),
                force_restart,
            ]
        )

        if not auth_db_info:
            self.logger.info("Workload stopped because auth db is missing.")
            try:
                self.workload.stop()
            except Exception:
                self.logger.warning("Could not stop Kyuubi workload even when auth db is missing.")
            return

        if tls_info and not self.workload.tls_ready():
            self.logger.info("Workload stopped because TLS is being enabled.")
            try:
                self.workload.stop()
            except Exception:
                self.logger.warning(
                    "Could not stop Kyuubi workload even when TLS is being enabled."
                )
            return

        if not should_restart:
            self.logger.info(
                "Workload restart skipped because the configurations have not changed."
            )
            return

        if not self.charm.refresh or not self.charm.refresh.workload_allowed_to_start:
            self.logger.info("Workload (re)start skipped; workload not allowed")
            return

        self.logger.info("Restarting kyuubi workload...")
        self.workload.restart()
