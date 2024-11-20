#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Kyuubi related event handlers."""

import ops
from ops import CharmBase

from constants import KYUUBI_CLIENT_RELATION_NAME, PEER_REL, VALID_EXPOSE_EXTERNAL_VALUES
from core.context import Context
from core.workload import KyuubiWorkloadBase
from events.base import BaseEventHandler, compute_status, defer_when_not_ready
from managers.kyuubi import KyuubiManager
from providers import KyuubiClientProvider
from utils.logging import WithLogging
from utils.service import ServiceUtil


class KyuubiEvents(BaseEventHandler, WithLogging):
    """Class implementing Kyuubih related event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: KyuubiWorkloadBase):
        super().__init__(charm, "kyuubi")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.workload)
        self.kyuubi_client = KyuubiClientProvider(self.charm, KYUUBI_CLIENT_RELATION_NAME)
        self.service_util = ServiceUtil(self.charm.model)

        self.framework.observe(self.charm.on.install, self._on_install)
        self.framework.observe(self.charm.on.kyuubi_pebble_ready, self._on_kyuubi_pebble_ready)
        self.framework.observe(self.charm.on.update_status, self._update_event)
        self.framework.observe(self.charm.on.config_changed, self._on_config_changed)

        # Peer relation events
        self.framework.observe(
            self.charm.on[PEER_REL].relation_joined, self._on_peer_relation_joined
        )
        self.framework.observe(
            self.charm.on[PEER_REL].relation_departed, self._on_peer_relation_departed
        )

    @compute_status
    def _on_install(self, event: ops.InstallEvent) -> None:
        """Handle the `on_install` event."""

    @compute_status
    @defer_when_not_ready
    def _on_config_changed(self, event: ops.ConfigChangedEvent) -> None:
        """Handle the on_config_changed event."""
        if not self.charm.unit.is_leader():
            return

        expose_external = self.charm.config.get("expose-external", "false")
        if expose_external not in VALID_EXPOSE_EXTERNAL_VALUES:
            self.logger.warning(f"Invalid value for expose-external: {expose_external}")
            return

        # Upon the change in the `external-expose` config option,
        # we want to reconcile the existing K8s service to reflect
        # the new desired service type.
        self.service_util.reconcile_services(expose_external)

        self.kyuubi.update(
            s3_info=self.context.s3,
            metastore_db_info=self.context.metastore_db,
            auth_db_info=self.context.auth_db,
            service_account_info=self.context.service_account,
            zookeeper_info=self.context.zookeeper,
        )

        # Check the newly created service is connectable
        if not self.service_util.is_service_connectable():
            self.logger.error("DEFER: not connectable, deferring now...")
            event.defer()
            return

        self.logger.error("DEFER: connectable, finishing execution now...")

    @compute_status
    def _update_event(self, event):
        """Handle the update event hook."""
        pass

    @compute_status
    @defer_when_not_ready
    def _on_kyuubi_pebble_ready(self, event: ops.PebbleReadyEvent):
        """Define and start a workload using the Pebble API."""
        self.logger.info("Kyuubi pebble service is ready.")
        self.kyuubi.update(
            s3_info=self.context.s3,
            metastore_db_info=self.context.metastore_db,
            auth_db_info=self.context.auth_db,
            service_account_info=self.context.service_account,
            zookeeper_info=self.context.zookeeper,
        )

    @compute_status
    def _on_peer_relation_joined(self, event: ops.RelationJoinedEvent):
        """Handle the peer relation joined event.

        This is necessary for updating status of all units upon scaling up/down.
        """
        self.logger.info("Kyuubi peer relation joined...")

    @compute_status
    def _on_peer_relation_departed(self, event: ops.RelationDepartedEvent):
        """Handle the peer relation departed event.

        This is necessary for updating status of all units upon scaling up/down.
        """
        self.logger.info("Kyuubi peer relation departed...")
