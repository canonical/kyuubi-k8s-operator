#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Action related event handlers."""

from ops import CharmBase
from ops.charm import ActionEvent

from constants import DEFAULT_ADMIN_USERNAME
from core.context import Context
from events.base import BaseEventHandler
from managers.auth import AuthenticationManager
from managers.kyuubi import KyuubiManager
from utils.logging import WithLogging
from workload.base import KyuubiWorkloadBase


class ActionEvents(BaseEventHandler, WithLogging):
    """Class implementing PostgreSQL metastore event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: KyuubiWorkloadBase):
        super().__init__(charm, "s3")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.workload)
        self.auth = AuthenticationManager(self.context.auth_db)

        self.framework.observe(self.charm.on.get_jdbc_endpoint_action, self._on_get_jdbc_endpoint)
        self.framework.observe(self.charm.on.get_password_action, self._on_get_password)
        self.framework.observe(self.charm.on.set_password_action, self._on_set_password)

    def _on_get_jdbc_endpoint(self, event: ActionEvent):
        result = {"endpoint": self.workload.get_jdbc_endpoint()}
        event.set_results(result)

    def _on_get_password(self, event: ActionEvent) -> None:
        """Returns the password for admin user."""
        if not self.context.is_authentication_enabled():
            event.fail(
                "The action can only be run when authentication is enabled. "
                "Please integrate kyuubi-k8s:auth-db with postgresql-k8s"
            )
            return
        password = self.auth.get_password(DEFAULT_ADMIN_USERNAME)
        event.set_results({"password": password})

    def _on_set_password(self, event: ActionEvent) -> None:
        """Set the password for the admin user."""
        if not self.context.is_authentication_enabled():
            event.fail(
                "The action can only be run when authentication is enabled. "
                "Please integrate kyuubi-k8s:auth-db with postgresql-k8s"
            )
            return

        # Only leader can write the new password
        if not self.unit.is_leader():
            event.fail("The action can be run only on leader unit")
            return

        password = self.auth.generate_password()

        if "password" in event.params:
            password = event.params["password"]

        if password == self.auth.get_password(DEFAULT_ADMIN_USERNAME):
            event.log("The old and new passwords are equal.")
            event.set_results({"password": password})
            return

        self.auth.set_password(DEFAULT_ADMIN_USERNAME, password)

        event.set_results({"password": password})
