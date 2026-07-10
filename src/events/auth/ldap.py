#!/usr/bin/env python3

# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

"""LDAP authentication event handlers for Kyuubi charm."""

from typing import TYPE_CHECKING

from charms.glauth_k8s.v0.ldap import (
    LdapReadyEvent,
    LdapUnavailableEvent,
)

from core.context import Context
from core.workload.kyuubi import KyuubiWorkload
from events.base import BaseEventHandler, defer_when_not_ready
from managers.kyuubi import KyuubiManager
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KyuubiCharm


class LDAPAuthenticationEvents(BaseEventHandler, WithLogging):
    """Class implementing LDAP authentication event hooks."""

    def __init__(self, charm: KyuubiCharm, context: Context, workload: KyuubiWorkload) -> None:
        super().__init__(charm, "ldap")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.charm, self.workload, self.context)

        self.framework.observe(
            self.context.ldap_requirer.on.ldap_ready,
            self._on_ldap_ready,
        )
        self.framework.observe(
            self.context.ldap_requirer.on.ldap_unavailable,
            self._on_ldap_unavailable,
        )

    @defer_when_not_ready
    def _on_ldap_ready(self, event: LdapReadyEvent) -> None:
        """Handle the event when LDAP integration is ready."""
        if not (ldap := self.context.ldap):
            self.logger.debug(f"ldap is {ldap}, deferring event...")
            event.defer()
            return
        self.kyuubi.update()

    @defer_when_not_ready
    def _on_ldap_unavailable(self, event: LdapUnavailableEvent) -> None:
        """Handle the event when LDAP integration is unavailable."""
        self.kyuubi.update(set_ldap_none=True)
        self.logger.info("LDAP authentication relation removed")
