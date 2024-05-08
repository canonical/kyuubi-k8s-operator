#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 Integration related event handlers."""

from ops import CharmBase


from utils.logging import WithLogging
from workload.base import KyuubiWorkloadBase
from events.base import BaseEventHandler, compute_status
from managers.kyuubi import KyuubiManager
from core.context import Context
from constants import POSTGRESQL_METASTORE_DB_REL, METASTORE_DATABASE_NAME
from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseCreatedEvent,
    DatabaseRequires,
)


class MetastoreEvents(BaseEventHandler, WithLogging):
    """Class implementing PostgreSQL metastore event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: KyuubiWorkloadBase):
        super().__init__(charm, "s3")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.workload)
        self.metastore_db =  DatabaseRequires(
            self, relation_name=POSTGRESQL_METASTORE_DB_REL, database_name=METASTORE_DATABASE_NAME
        )

        self.framework.observe(
            self.metastore_db.on.database_created, self._on_metastore_db_created
        )
        self.framework.observe(
            self.metastore_db.on.endpoints_changed, self._on_metastore_db_created
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
            service_account_info=self.context.service_account
        )

    @compute_status
    def _on_metastore_db_relation_removed(self, event) -> None:
        self.logger.info("Mestastore database relation removed")
        self.kyuubi.update(
            s3_info=self.context.s3,
            metastore_db_info=self.context.metastore_db,
            auth_db_info=self.context.auth_db,
            service_account_info=self.context.service_account
        )
