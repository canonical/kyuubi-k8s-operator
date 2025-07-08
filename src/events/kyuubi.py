#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Kyuubi related event handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import ops
from ops import SecretChangedEvent

from constants import DEFAULT_ADMIN_USERNAME, PEER_REL
from core.context import Context
from core.domain import DatabaseConnectionInfo
from core.workload.kyuubi import KyuubiWorkload
from events.base import BaseEventHandler, defer_when_not_ready
from managers.auth import AuthenticationManager
from managers.kyuubi import KyuubiManager
from managers.service import ServiceManager
from managers.tls import TLSManager
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KyuubiCharm


class KyuubiEvents(BaseEventHandler, WithLogging):
    """Class implementing Kyuubi related event hooks."""

    def __init__(self, charm: KyuubiCharm, context: Context, workload: KyuubiWorkload) -> None:
        super().__init__(charm, "kyuubi")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.charm, self.workload, self.context)
        self.service_manager = ServiceManager(
            namespace=self.charm.model.name,
            unit_name=self.charm.unit.name,
            app_name=self.charm.app.name,
        )

        self.tls_manager = TLSManager(context=self.context, workload=self.workload)

        self.framework.observe(self.charm.on.install, self._on_install)
        self.framework.observe(self.charm.on.kyuubi_pebble_ready, self._on_kyuubi_pebble_ready)
        self.framework.observe(self.charm.on.update_status, self._update_event)
        self.framework.observe(self.charm.on.config_changed, self._on_config_changed)
        self.framework.observe(self.charm.on.secret_changed, self._on_secret_changed)

        # Peer relation events
        self.framework.observe(
            self.charm.on[PEER_REL].relation_joined, self._on_peer_relation_joined
        )
        self.framework.observe(
            self.charm.on[PEER_REL].relation_departed, self._on_peer_relation_departed
        )

    def _on_install(self, _: ops.InstallEvent) -> None:
        """Handle the `on_install` event."""

    @defer_when_not_ready
    def _on_config_changed(self, event: ops.ConfigChangedEvent) -> None:
        """Handle the on_config_changed event."""
        if self.charm.unit.is_leader():
            if not self.context.cluster.relation:
                event.defer()
                return

            if not self.context.is_authentication_enabled():
                event.defer()
                return

            auth_manager = AuthenticationManager(
                cast(DatabaseConnectionInfo, self.context.auth_db)
            )
            if not auth_manager.user_exists(DEFAULT_ADMIN_USERNAME):
                event.defer()
                return

            # Create / update the managed service to reflect the service type in config
            if self.service_manager.reconcile_services(
                self.charm.config.expose_external, self.charm.config.loadbalancer_extra_annotations
            ):
                self.charm.provider_events.update_clients_endpoints()

            if (
                admin_password := self.charm.validate_and_get_admin_password()
            ) and admin_password != self.context.cluster.admin_password:
                auth_manager.set_password(username=DEFAULT_ADMIN_USERNAME, password=admin_password)
                self.context.cluster.update_admin_password(password=admin_password)

        self.kyuubi.update()

        # Check the newly created service endpoint is available
        if not self.service_manager.get_service_endpoint(
            expose_external=self.charm.config.expose_external
        ):
            self.logger.info(
                "Managed K8s service is not available yet; deferring config-changed event now..."
            )
            event.defer()
            return

        # Update the TLS private key in the peer app databag, so that peer-relation-changed can regenerate certificates
        self.charm.context.cluster.set_private_key(
            key.raw if (key := self.charm.validate_and_get_private_key()) else ""
        )

        # Check the newly created service endpoint is available
        if not (
            endpoint := self.service_manager.get_service_endpoint(
                expose_external=self.charm.config.expose_external
            )
        ):
            self.logger.info(
                "Managed K8s service is not available yet; deferring config-changed event now..."
            )
            event.defer()
            return
        else:
            self.logger.info(
                "Managed K8s service is available; completed handling config-changed event."
            )
            self.charm.context.unit_server.set_subject_common_name(endpoint.host)

    def _update_event(self, _):
        """Handle the update event hook."""
        self.kyuubi.update()

    @defer_when_not_ready
    def _on_kyuubi_pebble_ready(self, _: ops.PebbleReadyEvent):
        """Define and start a workload using the Pebble API."""
        self.logger.info("Kyuubi pebble service is ready.")
        self.kyuubi.update()

    def _on_peer_relation_joined(self, _: ops.RelationJoinedEvent):
        """Handle the peer relation joined event.

        This is necessary for updating status of all units upon scaling up/down.
        """
        self.logger.info("Kyuubi peer relation joined...")

    def _on_peer_relation_departed(self, _: ops.RelationDepartedEvent):
        """Handle the peer relation departed event.

        This is necessary for updating status of all units upon scaling up/down.
        """
        self.logger.info("Kyuubi peer relation departed...")

    def _on_secret_changed(self, event: SecretChangedEvent) -> None:
        """Reconfigure services on a secret changed event."""
        if not self.context.cluster.relation:
            event.defer()
            return

        if (
            event.secret.label
            and event.secret.label
            == self.context.cluster.data_interface._generate_secret_label(
                PEER_REL,
                self.context.cluster.relation.id,
                "extra",  # type: ignore
                # Changes with the https://github.com/canonical/data-platform-libs/issues/124
            )
        ):
            self.logger.info(f"Event secret label: {event.secret.label} updated!")
            return

        if (
            self.charm.config.tls_client_private_key
            and self.charm.config.tls_client_private_key == event.secret.id
            and (key := self.charm.validate_and_get_private_key())
        ):
            self.context.cluster.set_private_key(key.raw)

        if (
            self.charm.unit.is_leader()
            and self.charm.config.system_users
            and self.charm.config.system_users == event.secret.id
        ):
            if not self.context.is_authentication_enabled():
                event.defer()
                return

            auth_manager = AuthenticationManager(
                cast(DatabaseConnectionInfo, self.context.auth_db)
            )
            if not auth_manager.user_exists(DEFAULT_ADMIN_USERNAME):
                event.defer()
                return

            if (
                admin_password := self.charm.validate_and_get_admin_password()
            ) and admin_password != self.context.cluster.admin_password:
                auth_manager.set_password(username=DEFAULT_ADMIN_USERNAME, password=admin_password)
                self.context.cluster.update_admin_password(password=admin_password)

    @defer_when_not_ready
    def _on_peer_relation_changed(self, _: ops.RelationChangedEvent):
        """Handle the peer relation changed event."""
        self.logger.info("Kyuubi peer relation changed...")
        self.charm.tls_events.refresh_tls_certificates_event.emit()
