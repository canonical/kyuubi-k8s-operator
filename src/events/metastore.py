#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Metastore database related event handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseCreatedEvent,
    DatabaseRequirerEventHandlers,
)

from constants import HIVE_SCHEMA_VERSION
from core.context import Context
from core.workload.kyuubi import KyuubiWorkload
from events.base import BaseEventHandler, compute_status, defer_when_not_ready
from managers.hive_metastore import HiveMetastoreManager
from managers.kyuubi import KyuubiManager
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KyuubiCharm


class MetastoreEvents(BaseEventHandler, WithLogging):
    """Class implementing PostgreSQL metastore event hooks."""

    def __init__(self, charm: KyuubiCharm, context: Context, workload: KyuubiWorkload) -> None:
        super().__init__(charm, "metastore")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.workload, self.context)
        self.metastore_manager = HiveMetastoreManager(self.workload)
        self.metatstore_db_handler = DatabaseRequirerEventHandlers(
            self.charm, self.context.metastore_db_requirer
        )

        self.framework.observe(
            self.metatstore_db_handler.on.database_created, self._on_metastore_db_created
        )
        self.framework.observe(
            self.metatstore_db_handler.on.endpoints_changed, self._on_metastore_db_created
        )
        self.framework.observe(
            self.charm.on.metastore_db_relation_broken, self._on_metastore_db_relation_removed
        )

    @compute_status
    @defer_when_not_ready
    def _on_metastore_db_created(self, event: DatabaseCreatedEvent) -> None:
        """Handle event when metastore database is created."""
        self.logger.info("Metastore database created...")
        self.kyuubi.update()
        self.metastore_manager.initialize(
            schema_version=HIVE_SCHEMA_VERSION, username=event.username, password=event.password
        )

    @compute_status
    def _on_metastore_db_relation_removed(self, event) -> None:
        """Handle event when metastore database relation is removed."""
        self.logger.info("Mestastore database relation removed")
        self.kyuubi.update(set_metastore_db_none=True)
