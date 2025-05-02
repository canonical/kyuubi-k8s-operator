#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Authentication related event handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseCreatedEvent,
    DatabaseRequirerEventHandlers,
)

from core.context import Context
from core.workload.kyuubi import KyuubiWorkload
from events.base import BaseEventHandler, compute_status, defer_when_not_ready
from managers.auth import AuthenticationManager
from managers.kyuubi import KyuubiManager
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KyuubiCharm


class AuthenticationEvents(BaseEventHandler, WithLogging):
    """Class implementing PostgreSQL metastore event hooks."""

    def __init__(self, charm: KyuubiCharm, context: Context, workload: KyuubiWorkload) -> None:
        super().__init__(charm, "s3")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.workload, self.context)
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
    @defer_when_not_ready
    def _on_auth_db_created(self, event: DatabaseCreatedEvent) -> None:
        """Handle the event when authentication database is created."""
        if not (auth_db := self.context.auth_db) or auth_db is None:
            event.defer()
            return

        self.logger.info("Authentication database created...")
        auth = AuthenticationManager(auth_db, self.context)
        auth.prepare_auth_db()
        self.kyuubi.update()

    @compute_status
    def _on_auth_db_endpoints_changed(self, event) -> None:
        """Handle the event when authentication database endpoints are changed."""
        self.logger.info("Authentication database endpoints changed...")
        self.kyuubi.update()

    @compute_status
    def _on_auth_db_relation_removed(self, event) -> None:
        """Handle the event when authentication database relation is removed."""
        self.logger.info("Authentication database relation removed")
        self.kyuubi.update(set_auth_db_none=True)

    @compute_status
    def _on_auth_db_relation_departed(self, event) -> None:
        """Handle the event when the authentication database relation is departed."""
        self.logger.info("Authentication database relation departed")
