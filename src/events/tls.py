#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Event handler for related applications on the `certificates` relation interface."""

from __future__ import annotations

import base64
import os
import re
from typing import TYPE_CHECKING

from charms.tls_certificates_interface.v3.tls_certificates import (
    CertificateAvailableEvent,
    TLSCertificatesRequiresV3,
    generate_csr,
    generate_private_key,
)
from ops.charm import ActionEvent, RelationBrokenEvent, RelationCreatedEvent, RelationJoinedEvent
from ops.framework import EventBase

from core.context import Context
from core.workload.kyuubi import KyuubiWorkload
from events.base import BaseEventHandler, compute_status, defer_when_not_ready
from managers.kyuubi import KyuubiManager
from managers.tls import TLSManager
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KyuubiCharm


class TLSEvents(BaseEventHandler, WithLogging):
    """Event handlers for related applications on the `certificates` relation interface."""

    def __init__(self, charm: KyuubiCharm, context: Context, workload: KyuubiWorkload):
        super().__init__(charm, "tls")
        self.charm = charm
        self.context = context
        self.workload = workload
        self.kyuubi = KyuubiManager(self.workload, self.context)
        self.tls_manager = TLSManager(context, workload)
        self.certificates = TLSCertificatesRequiresV3(self.charm, "certificates")

        self.framework.observe(
            getattr(self.charm.on, "certificates_relation_created"), self._on_certificates_created
        )
        self.framework.observe(
            getattr(self.charm.on, "certificates_relation_joined"), self._on_certificates_joined
        )
        self.framework.observe(
            getattr(self.certificates.on, "certificate_available"), self._on_certificate_available
        )
        self.framework.observe(
            getattr(self.certificates.on, "certificate_expiring"), self._on_certificate_expiring
        )
        self.framework.observe(
            getattr(self.charm.on, "certificates_relation_broken"), self._on_certificates_broken
        )

        self.framework.observe(
            getattr(self.charm.on, "set_tls_private_key_action"), self._set_tls_private_key
        )

    def _on_certificates_created(self, event: RelationCreatedEvent) -> None:
        """Handler for `certificates_relation_created` event."""
        if not self.charm.unit.is_leader():
            return

        self.context.cluster.update({"tls": "enabled"})

    def _on_certificates_joined(self, event: RelationJoinedEvent) -> None:
        """Handler for `certificates_relation_joined` event."""
        if not self.context.cluster.tls:
            self.logger.debug(
                "certificates relation joined - tls not enabled and not switching encryption - deferring"
            )
            event.defer()
            return

        # generate unit private key if not already created by action
        if not self.context.unit_server.private_key:
            self.context.unit_server.update(
                {"private-key": generate_private_key().decode("utf-8")}
            )

        # generate unit key/truststore password if not already created by action
        self.context.unit_server.update(
            {
                "keystore-password": self.context.unit_server.keystore_password
                or self.workload.generate_password(),  # type: ignore
                "truststore-password": self.context.unit_server.truststore_password
                or self.workload.generate_password(),  # type: ignore
            }
        )
        subject = self.tls_manager.get_subject()
        sans = self.tls_manager.build_sans()

        self.logger.info(f"ip: {sans.sans_ip} tls: {sans.sans_dns}")

        csr = generate_csr(
            private_key=self.context.unit_server.private_key.encode("utf-8"),
            subject=subject,
            sans_ip=sans.sans_ip,
            sans_dns=sans.sans_dns,
        )

        self.context.unit_server.update({"csr": csr.decode("utf-8").strip()})

        self.certificates.request_certificate_creation(certificate_signing_request=csr)

    @compute_status
    def _on_certificate_available(self, event: CertificateAvailableEvent) -> None:
        """Handler for `certificates_available` event after provider updates signed certs."""
        # avoid setting tls files and restarting
        if not self.workload.ready():
            event.defer()
            return

        if event.certificate_signing_request != self.context.unit_server.csr:
            self.logger.error("Can't use certificate, found unknown CSR")
            return

        self.context.unit_server.update(
            {"certificate": event.certificate, "ca-cert": event.ca, "ca": ""}
        )
        self._cleanup_old_ca_field()

        self.tls_manager.set_private_key()
        self.tls_manager.set_ca()
        self.tls_manager.set_certificate()
        self.tls_manager.set_truststore()
        self.tls_manager.set_p12_keystore()

        self.kyuubi.update(force_restart=True)

    @compute_status
    def _on_certificate_expiring(self, _: EventBase) -> None:
        """Handler for `certificates_expiring` event when certs need renewing."""
        if not (self.context.unit_server.private_key or self.context.unit_server.csr):
            self.logger.error("Missing unit private key and/or old csr")
            return

        subject = os.uname()[1]
        sans = self.tls_manager.build_sans()

        new_csr = generate_csr(
            private_key=self.context.unit_server.private_key.encode("utf-8"),
            subject=subject,
            sans_ip=sans.sans_ip,
            sans_dns=sans.sans_dns,
        )

        self.certificates.request_certificate_renewal(
            old_certificate_signing_request=self.context.unit_server.csr.encode("utf-8"),
            new_certificate_signing_request=new_csr,
        )

        self.context.unit_server.update({"csr": new_csr.decode("utf-8").strip()})

    @compute_status
    @defer_when_not_ready
    def _on_certificates_broken(self, event: RelationBrokenEvent) -> None:
        """Handler for `certificates_relation_broken` event."""
        if not self.workload.ready():
            event.defer()
            return

        self.context.unit_server.update({"csr": "", "certificate": "", "ca-cert": "", "ca": ""})
        self._cleanup_old_ca_field()

        # remove all existing keystores from the unit so we don't preserve certs
        self.tls_manager.remove_stores()

        if not self.charm.unit.is_leader():
            return

        self.context.cluster.update({"tls": ""})
        self.kyuubi.update(set_tls_none=True)

    def _set_tls_private_key(self, event: ActionEvent) -> None:
        """Handler for `set-tls-private-key` event when user manually specifies private-keys for a unit."""
        key = event.params.get("internal-key") or generate_private_key().decode("utf-8")
        private_key = (
            key
            if re.match(r"(-+(BEGIN|END) [A-Z ]+-+)", key)
            else base64.b64decode(key).decode("utf-8")
        )

        self.context.unit_server.update({"private-key": private_key})
        self._on_certificate_expiring(event)

    def _cleanup_old_ca_field(self) -> None:
        """In order to ensure backwards compatibility, we keep old secrets until the first time they are updated.

        This will allow to safely roll back soon after an upgrade.
        """
        if self.context.unit_server.relation_data.get("ca"):
            self.context.unit_server.update({"ca": ""})
