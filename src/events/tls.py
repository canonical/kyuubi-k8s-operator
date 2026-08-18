#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Event handler for related applications on the `certificates` relation interface."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from charms.certificate_transfer_interface.v0.certificate_transfer import (
    CertificateAvailableEvent as TransferredCertificatesAvailableEvent,
)
from charms.certificate_transfer_interface.v0.certificate_transfer import (
    CertificateRemovedEvent as TransferredCertificatesRemovedEvent,
)
from charms.certificate_transfer_interface.v0.certificate_transfer import (
    CertificateTransferRequires,
)
from charms.tls_certificates_interface.v4.tls_certificates import (
    CertificateAvailableEvent,
    CertificateRequestAttributes,
    TLSCertificatesRequiresV4,
)
from ops import EventSource
from ops.charm import RelationBrokenEvent, RelationCreatedEvent
from ops.framework import EventBase

from constants import CERTIFICATES_TRANSFER_RELATION_NAME, TLS_REL
from core.context import Context
from core.workload.kyuubi import KyuubiWorkload
from events.base import BaseEventHandler, defer_when_not_ready
from managers.kyuubi import KyuubiManager
from managers.tls import TLSManager
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KyuubiCharm


class RefreshTLSCertificatesEvent(EventBase):
    """Event for refreshing peer TLS certificates."""


class TLSEvents(BaseEventHandler, WithLogging):
    """Event handlers for related applications on the `certificates` relation interface."""

    refresh_tls_certificates_event = EventSource(RefreshTLSCertificatesEvent)

    def __init__(self, charm: KyuubiCharm, context: Context, workload: KyuubiWorkload):
        super().__init__(charm, "tls")
        self.charm = charm
        self.context = context
        self.workload = workload
        self.kyuubi = KyuubiManager(self.charm, self.workload, self.context)
        self.tls_manager = TLSManager(context, workload)

        common_name = self.tls_manager.get_kyuubi_subject_name()
        sans = self.tls_manager.build_kyuubi_sans()
        sans_ip = frozenset(sans.sans_ip)
        sans_dns = frozenset(sans.sans_dns)
        private_key = self.charm.validate_and_get_private_key()
        self.certificates = TLSCertificatesRequiresV4(
            charm=self.charm,
            relationship_name=TLS_REL,
            certificate_requests=[
                CertificateRequestAttributes(
                    common_name=common_name,
                    sans_ip=sans_ip,
                    sans_dns=sans_dns,
                )
            ],
            private_key=private_key,
            refresh_events=[self.refresh_tls_certificates_event],
        )
        self.certificates_transfer = CertificateTransferRequires(
            charm=self.charm,
            relationship_name=CERTIFICATES_TRANSFER_RELATION_NAME,
        )

        self.framework.observe(
            getattr(self.charm.on, "certificates_relation_created"),
            self._on_kyuubi_server_certificates_created,
        )
        self.framework.observe(
            getattr(self.certificates.on, "certificate_available"),
            self._on_kyuubi_server_certificate_available,
        )
        self.framework.observe(
            getattr(self.charm.on, "certificates_relation_broken"),
            self._on_kyuubi_server_certificates_broken,
        )
        self.framework.observe(
            getattr(self.certificates_transfer.on, "certificate_available"),
            self._on_transferred_certificates_available,
        )
        self.framework.observe(
            getattr(self.certificates_transfer.on, "certificate_removed"),
            self._on_transferred_certificates_removed,
        )

    def _on_kyuubi_server_certificates_created(self, _: RelationCreatedEvent) -> None:
        """Handler for `certificates_relation_created` event."""
        if not self.charm.unit.is_leader():
            return

        self.context.cluster.update({"tls": "enabled"})

    def _on_kyuubi_server_certificate_available(self, event: CertificateAvailableEvent) -> None:
        """Handler for `certificates_available` event after provider updates signed certs."""
        # avoid setting tls files and restarting
        if not self.workload.ready():
            event.defer()
            return

        _, private_key = self.certificates.get_assigned_certificate(
            self.certificates.certificate_requests[0]
        )
        if private_key and private_key.raw != self.context.unit_server.private_key:
            self.context.unit_server.update({"private-key": private_key.raw})

        # generate unit key/truststore password if not already created
        self.context.unit_server.update(
            {
                "keystore-password": self.context.unit_server.keystore_password
                or self.workload.generate_password(),  # type: ignore
                "truststore-password": self.context.unit_server.truststore_password
                or self.workload.generate_password(),  # type: ignore
            }
        )
        self.context.unit_server.update(
            {"certificate": event.certificate.raw, "ca-cert": event.ca.raw}
        )

        self.tls_manager.set_private_key()
        self.tls_manager.set_kyuubi_server_ca()
        self.tls_manager.set_kyuubi_server_certificate()
        self.tls_manager.set_kyuubi_server_truststore()
        self.tls_manager.set_kyuubi_server_p12_keystore()
        self.kyuubi.update(force_restart=True)

        self.charm.provider_events.update_clients_endpoints()


    # def generate_alias_for_certificate(
    #     self, certificate: x509.Certificate, relation_id: int
    # ) -> str:
    #     """Generates an alias for the given certificate based on its SHA256 fingerprint."""
    #     fingerprint = certificate.fingerprint(hashes.SHA256()).hex()[:16]
    #     return f"transferred-cert-{relation_id}-{fingerprint}"


    def _on_transferred_certificates_available(
        self, event: TransferredCertificatesAvailableEvent
    ) -> None:
        """Handler for `certificate_set_updated` event after provider updates signed certs."""
        # avoid setting tls files and restarting
        if not self.workload.ready():
            event.defer()
            return

        # generate unit truststore password if not already created (for transferred certificates)
        self.context.unit_server.update(
            {
                "truststore-password": self.context.unit_server.truststore_password
                or self.workload.generate_password(),  # type: ignore
            }
        )

        self.context.unit_server.update(
            {f"transferred-certificates-{event.relation_id}": event.ca}
        )

        self.tls_manager.set_transferred_certificates(relation_id=cast(int, event.relation_id))
        self.tls_manager.set_transferred_certificates_truststore(
            relation_id=cast(int, event.relation_id)
        )
        self.kyuubi.update(force_restart=True)

    @defer_when_not_ready
    def _on_kyuubi_server_certificates_broken(self, event: RelationBrokenEvent) -> None:
        """Handler for `certificates_relation_broken` event."""
        if not self.workload.ready():
            event.defer()
            return

        self.context.unit_server.update({"certificate": "", "ca-cert": ""})

        # remove all existing keystores from the unit so we don't preserve certs
        self.tls_manager.delete_kyuubi_server_certificates()
        self.kyuubi.update(set_frontend_tls_none=True)

        if self.charm.unit.is_leader():
            self.context.cluster.update({"tls": ""})
            self.charm.provider_events.update_clients_endpoints()

    @defer_when_not_ready
    def _on_transferred_certificates_removed(
        self, event: TransferredCertificatesRemovedEvent
    ) -> None:
        """Handler for `certificates_relation_broken` event."""
        if not self.workload.ready():
            event.defer()
            return
        relation_id = event.relation_id
        self.context.unit_server.update({f"transferred-certificates-{relation_id}": ""})

        # remove all existing keystores from the unit so we don't preserve certs (for transferred certificates)
        self.tls_manager.delete_transferred_certificates(relation_id=relation_id)
        self.kyuubi.update(set_backend_tls_none=True)
