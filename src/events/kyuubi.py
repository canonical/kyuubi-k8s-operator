#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Kyuubi related event handlers."""

import ops
from ops import CharmBase

from core.context import Context
from core.domain import Status
from events.base import BaseEventHandler
from managers.kyuubi import KyuubiManager
from utils.logging import WithLogging
from workload.base import KyuubiWorkloadBase


class KyuubiEvents(BaseEventHandler, WithLogging):
    """Class implementing S3 Integration event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: KyuubiWorkloadBase):
        super().__init__(charm, "kyuubi")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.workload)

        self.framework.observe(self.charm.on.install, self._on_install)
        self.framework.observe(self.charm.on.kyuubi_pebble_ready, self._on_kyuubi_pebble_ready)
        self.framework.observe(self.charm.on.update_status, self._update_event)
        self.framework.observe(self.charm.on.config_changed, self._on_config_changed)

    def _on_install(self, event: ops.InstallEvent) -> None:
        """Handle the `on_install` event."""
        self.unit.status = Status.WAITING_PEBBLE.value

    def _on_config_changed(self, event: ops.ConfigChangedEvent) -> None:
        """Handle the on_config_changed event."""
        if not self.unit.is_leader():
            return

        self.kyuubi.update(
            s3_info=self.context.s3,
            metastore_db_info=self.context.metastore_db,
            auth_db_info=self.context.auth_db,
            service_account_info=self.context.service_account,
        )

    def _update_event(self, _):
        """Handle the update event hook."""
        self.unit.status = self.get_app_status()

    def _on_kyuubi_pebble_ready(self, event: ops.PebbleReadyEvent):
        """Define and start a workload using the Pebble API."""
        self.logger.info("Kyuubi pebble service is ready.")
        self.kyuubi.update(
            s3_info=self.context.s3,
            metastore_db_info=self.context.metastore_db,
            auth_db_info=self.context.auth_db,
            service_account_info=self.context.service_account,
        )
