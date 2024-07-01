#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Authentication related event handlers."""

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseCreatedEvent,
    DatabaseRequirerEventHandlers,
)
from ops import CharmBase

from core.context import Context
from core.workload import KyuubiWorkloadBase
from events.base import BaseEventHandler, compute_status, defer_when_not_read
from managers.auth import AuthenticationManager
from managers.kyuubi import KyuubiManager
from utils.logging import WithLogging


class AuthenticationEvents(BaseEventHandler, WithLogging):
    """Class implementing PostgreSQL metastore event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: KyuubiWorkloadBase):
        super().__init__(charm, "s3")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.workload)
        self.auth_db_handler = DatabaseRequirerEventHandlers(
            self.charm, self.context.auth_db_requirer
        )

        self.framework.observe(self.auth_db_handler.on.database_created, self._on_auth_db_created)
        self.framework.observe(
            self.auth_db_handler.on.endpoints_changed, self._on_auth_db_endpoints_changed
        )
        self.framework.observe(
            self.charm.on.auth_db_relation_broken, self._on_auth_db_relation_removed
        )
        self.framework.observe(
            self.charm.on.auth_db_relation_departed, self._on_auth_db_relation_departed
        )

    @compute_status
    @defer_when_not_read
    def _on_auth_db_created(self, event: DatabaseCreatedEvent) -> None:
        """Handle the event when authentication database is created."""
        self.logger.info("Authentication database created...")
        auth = AuthenticationManager(self.context.auth_db)
        auth.prepare_auth_db()
        self.kyuubi.update(
            s3_info=self.context.s3,
            metastore_db_info=self.context.metastore_db,
            auth_db_info=self.context.auth_db,
            service_account_info=self.context.service_account,
        )

    @compute_status
    def _on_auth_db_endpoints_changed(self, event) -> None:
        """Handle the event when authentication database endpoints are changed."""
        self.logger.info("Authentication database endpoints changed...")
        self.kyuubi.update(
            s3_info=self.context.s3,
            metastore_db_info=self.context.metastore_db,
            auth_db_info=self.context.auth_db,
            service_account_info=self.context.service_account,
        )

    @compute_status
    def _on_auth_db_relation_removed(self, event) -> None:
        """Handle the event when authentication database relation is removed."""
        self.logger.info("Authentication database relation removed")
        self.kyuubi.update(
            s3_info=self.context.s3,
            metastore_db_info=self.context.metastore_db,
            auth_db_info=None,
            service_account_info=self.context.service_account,
        )

    @compute_status
    def _on_auth_db_relation_departed(self, event) -> None:
        """Handle the event when the authentication database relation is departed.

        Until this point, the relation data is still there and hence the credentials
        can be fetched for one last time in order to remove authentication database.
        """
        self.logger.info("Authentication database relation departed")
        auth = AuthenticationManager(self.context.auth_db)
        auth.remove_auth_db()
