#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Kyuubi related event handlers."""

from datetime import datetime

import ops
from charms.data_platform_libs.v0.data_models import TypedCharmBase
from ops import SecretChangedEvent

from constants import KYUUBI_CLIENT_RELATION_NAME, PEER_REL
from core.context import Context
from core.workload import KyuubiWorkloadBase
from events.base import BaseEventHandler, compute_status, defer_when_not_ready
from managers.kyuubi import KyuubiManager
from managers.service import ServiceManager
from managers.tls import TLSManager
from providers import KyuubiClientProvider
from utils.logging import WithLogging


class KyuubiEvents(BaseEventHandler, WithLogging):
    """Class implementing Kyuubih related event hooks."""

    def __init__(self, charm: TypedCharmBase, context: Context, workload: KyuubiWorkloadBase):
        super().__init__(charm, "kyuubi")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.workload, self.context)
        self.kyuubi_client = KyuubiClientProvider(self.charm, KYUUBI_CLIENT_RELATION_NAME)
        self.service_manager = ServiceManager(
            namespace=self.charm.model.name,
            unit_name=self.charm.unit.name,
            app_name=self.charm.app.name,
        )

        self.tls_manager = TLSManager(context=self.context, workload=self.workload)

        # self.tls_events = TLSEvents(self.charm, self.context, self.workload)

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
            self.charm.on[PEER_REL].relation_changed, self._on_peer_relation_changed
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
        if self.charm.unit.is_leader():
            # Create / update the managed service to reflect the service type in config
            self.service_manager.reconcile_services(self.charm.config.expose_external)

        self.kyuubi.update()

        # Check the newly created service is connectable
        if not self.service_manager.get_service_endpoint():
            self.logger.info(
                "Managed K8s service is not available yet; deferring config-changed event now..."
            )
            event.defer()
            return

        self.logger.info(
            "Managed K8s service is available; completed handling config-changed event."
        )

    @compute_status
    def _update_event(self, event):
        """Handle the update event hook."""
        pass

    @compute_status
    @defer_when_not_ready
    def _on_kyuubi_pebble_ready(self, event: ops.PebbleReadyEvent):
        """Define and start a workload using the Pebble API."""
        self.logger.info("Kyuubi pebble service is ready.")
        self.kyuubi.update()

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

    @compute_status
    def _on_secret_changed(self, event: SecretChangedEvent) -> None:
        """Reconfigure services on a secret changed event."""
        if not event.secret.label:
            return

        if not self.context.cluster.relation:
            return

        if event.secret.label == self.context.cluster.data_interface._generate_secret_label(
            PEER_REL,
            self.context.cluster.relation.id,
            "extra",  # type:ignore noqa  -- Changes with the https://github.com/canonical/data-platform-libs/issues/124
        ):
            self.logger.info(f"Event secret label: {event.secret.label} updated!")

    @compute_status
    def _on_peer_relation_changed(self, event: ops.RelationDepartedEvent):
        """Handle the peer relation changed event."""
        self.logger.info("Kyuubi peer relation changed...")

        current_sans = self.tls_manager.get_current_sans()

        current_sans_ip = set(current_sans.sans_ip) if current_sans else set()
        expected_sans_ip = set(self.tls_manager.build_sans().sans_ip) if current_sans else set()
        sans_ip_changed = current_sans_ip ^ expected_sans_ip

        current_sans_dns = set(current_sans.sans_dns) if current_sans else set()
        expected_sans_dns = set(self.tls_manager.build_sans().sans_dns) if current_sans else set()
        sans_dns_changed = current_sans_dns ^ expected_sans_dns
        # TODO properly test this function when external access is merged.
        if sans_ip_changed or sans_dns_changed:
            self.logger.info(
                (
                    f'SERVER {self.charm.unit.name.split("/")[1]} updating certificate SANs - '
                    f"OLD SANs IP = {current_sans_ip - expected_sans_ip}, "
                    f"NEW SANs IP = {expected_sans_ip - current_sans_ip}, "
                    f"OLD SANs DNS = {current_sans_dns - expected_sans_dns}, "
                    f"NEW SANs DNS = {expected_sans_dns - current_sans_dns}"
                )
            )
            self.charm.tls_events.certificates.on.certificate_expiring.emit(  # type: ignore
                certificate=self.context.unit_server.certificate,
                expiry=datetime.now().isoformat(),
            )  # new cert will eventually be dynamically loaded by the server
            self.context.unit_server.update(
                {"certificate": ""}
            )  # ensures only single requested new certs, will be replaced on new certificate-available event

            return  # early return here to ensure new node cert arrives before updating the clients
