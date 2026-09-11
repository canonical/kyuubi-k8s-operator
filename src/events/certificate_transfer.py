#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Event handler for related applications on the `certificates` relation interface."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from charms.certificate_transfer_interface.v0.certificate_transfer import (
    CertificateAvailableEvent,
    CertificateRemovedEvent,
    CertificateTransferRequires,
)
from cryptography import x509
from cryptography.hazmat.primitives import hashes

from constants import CERTIFICATES_TRANSFER_RELATION_NAME
from core.context import Context
from core.workload.kyuubi import KyuubiWorkload
from events.base import BaseEventHandler, defer_when_not_ready
from managers.kyuubi import KyuubiManager
from managers.tls import TLSManager
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KyuubiCharm


class CertificatesTransferEvents(BaseEventHandler, WithLogging):
    """Event handlers for related applications on the `certificate_transfer` relation interface."""

    def __init__(self, charm: KyuubiCharm, context: Context, workload: KyuubiWorkload):
        super().__init__(charm, "certificate_transfer")
        self.charm = charm
        self.context = context
        self.workload = workload
        self.kyuubi = KyuubiManager(self.charm, self.workload, self.context)
        self.tls_manager = TLSManager(context, workload)

        self.certificates_transfer = CertificateTransferRequires(
            charm=self.charm,
            relationship_name=CERTIFICATES_TRANSFER_RELATION_NAME,
        )

        self.framework.observe(
            getattr(self.certificates_transfer.on, "certificate_available"),
            self._on_transferred_certificates_available,
        )
        self.framework.observe(
            getattr(self.certificates_transfer.on, "certificate_removed"),
            self._on_transferred_certificates_removed,
        )

    def _on_transferred_certificates_available(self, event: CertificateAvailableEvent) -> None:
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

        relation_id = cast(int, event.relation_id)
        certificates = {
            self.generate_alias_for_certificate(certificate, relation_id): certificate
            for certificate in x509.load_pem_x509_certificates(event.ca.encode())
        }
        self.tls_manager.set_truststore_certificates(certificates)
        self.kyuubi.update(force_restart=True)

    @defer_when_not_ready
    def _on_transferred_certificates_removed(self, event: CertificateRemovedEvent) -> None:
        """Handler for `certificates_relation_broken` event."""
        if not self.workload.ready():
            event.defer()
            return
        relation_id = event.relation_id
        ca_bundle = self.context.unit_server.get_transferred_certificates_for_relation(relation_id)
        aliases = (
            [
                self.generate_alias_for_certificate(certificate, relation_id)
                for certificate in x509.load_pem_x509_certificates(ca_bundle.encode())
            ]
            if ca_bundle
            else []
        )
        self.context.unit_server.update({f"transferred-certificates-{relation_id}": ""})

        self.tls_manager.delete_truststore_certificates(aliases)
        self.kyuubi.update(set_backend_tls_none=True)

    def generate_alias_for_certificate(
        self, certificate: x509.Certificate, relation_id: int
    ) -> str:
        """Generate an alias for the given certificate based on its SHA256 fingerprint."""
        fingerprint = certificate.fingerprint(hashes.SHA256()).hex()[:16]
        return f"transferred-cert-{relation_id}-{fingerprint}"
