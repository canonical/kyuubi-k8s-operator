#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Metastore database related event handlers."""

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseCreatedEvent,
)
from ops import CharmBase

from core.context import Context
from events.base import BaseEventHandler, compute_status
from managers.kyuubi import KyuubiManager
from utils.logging import WithLogging
from workload.base import KyuubiWorkloadBase


class MetastoreEvents(BaseEventHandler, WithLogging):
    """Class implementing PostgreSQL metastore event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: KyuubiWorkloadBase):
        super().__init__(charm, "metastore")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.workload)

        self.framework.observe(
            self.charm.metastore_db.on.database_created, self._on_metastore_db_created
        )
        self.framework.observe(
            self.charm.metastore_db.on.endpoints_changed, self._on_metastore_db_created
        )
        self.framework.observe(
            self.charm.on.metastore_db_relation_broken, self._on_metastore_db_relation_removed
        )

    @compute_status
    def _on_metastore_db_created(self, event: DatabaseCreatedEvent) -> None:
        self.logger.info("Metastore database created...")
        self.kyuubi.update(
            s3_info=self.context.s3,
            metastore_db_info=self.context.metastore_db,
            auth_db_info=self.context.auth_db,
            service_account_info=self.context.service_account,
        )

    @compute_status
    def _on_metastore_db_relation_removed(self, event) -> None:
        self.logger.info("Mestastore database relation removed")
        self.kyuubi.update(
            s3_info=self.context.s3,
            metastore_db_info=self.context.metastore_db,
            auth_db_info=self.context.auth_db,
            service_account_info=self.context.service_account,
        )
