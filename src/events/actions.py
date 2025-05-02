#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Action related event handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from ops.charm import ActionEvent

from constants import DEFAULT_ADMIN_USERNAME
from core.context import Context
from core.domain import DatabaseConnectionInfo, Status
from core.workload.kyuubi import KyuubiWorkload
from events.base import BaseEventHandler
from managers.auth import AuthenticationManager
from managers.kyuubi import KyuubiManager
from managers.service import ServiceManager
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KyuubiCharm


class ActionEvents(BaseEventHandler, WithLogging):
    """Class implementing charm action event hooks."""

    def __init__(self, charm: KyuubiCharm, context: Context, workload: KyuubiWorkload):
        super().__init__(charm, "action-events")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.workload, self.context)
        self.service_manager = ServiceManager(
            namespace=self.charm.model.name,
            unit_name=self.charm.unit.name,
            app_name=self.charm.app.name,
        )

        self.framework.observe(self.charm.on.get_jdbc_endpoint_action, self._on_get_jdbc_endpoint)
        self.framework.observe(self.charm.on.get_password_action, self._on_get_password)
        self.framework.observe(self.charm.on.set_password_action, self._on_set_password)

    def _on_get_jdbc_endpoint(self, event: ActionEvent):
        """Action event handler that returns back with a JDBC endpoint."""
        failure_conditions = [
            (
                lambda: not self.charm.unit.is_leader(),
                "Action must be ran on the application leader",
            ),
            (
                lambda: not self.workload.ready(),
                "The action failed because the workload is not ready yet.",
            ),
            (
                lambda: self.get_app_status() != Status.ACTIVE.value,
                "The action failed because the charm is not in active state.",
            ),
        ]

        for check, msg in failure_conditions:
            if check():
                self.logger.error(msg)
                event.set_results({"error": msg})
                event.fail(msg)
                return

        address = self.service_manager.get_service_endpoint(
            expose_external=self.charm.config.expose_external
        )
        if address is None:
            event.fail(
                "The action failed because the Kubernetes service is not available at the moment."
            )
            return
        endpoint = f"jdbc:hive2://{address.host}:{address.port}/"
        result = {"endpoint": endpoint}
        event.set_results(result)

    def _on_get_password(self, event: ActionEvent) -> None:
        """Returns the password for admin user."""
        failure_conditions = [
            (
                lambda: not self.charm.unit.is_leader(),
                "Action must be ran on the application leader",
            ),
            (
                lambda: not self.context.is_authentication_enabled(),
                "The action can only be run when authentication is enabled. "
                "Please integrate kyuubi-k8s:auth-db with postgresql-k8s",
            ),
            (
                lambda: not self.workload.ready(),
                "The action failed because the workload is not ready yet.",
            ),
            (
                lambda: self.get_app_status() != Status.ACTIVE.value,
                "The action failed because the charm is not in active state.",
            ),
        ]

        for check, msg in failure_conditions:
            if check():
                self.logger.error(msg)
                event.set_results({"error": msg})
                event.fail(msg)
                return

        auth = AuthenticationManager(
            cast(DatabaseConnectionInfo, self.context.auth_db), self.context
        )
        password = auth.get_password(DEFAULT_ADMIN_USERNAME)
        if password is None:
            event.fail(
                f"The action failed because the password could not be fetched for {DEFAULT_ADMIN_USERNAME}."
            )
            return
        event.set_results({"password": password})

    def _on_set_password(self, event: ActionEvent) -> None:
        """Set the password for the admin user."""
        failure_conditions = [
            (
                lambda: not self.charm.unit.is_leader(),
                "Action must be ran on the application leader",
            ),
            (
                lambda: not self.context.is_authentication_enabled(),
                "The action can only be run when authentication is enabled. "
                "Please integrate kyuubi-k8s:auth-db with postgresql-k8s",
            ),
            (
                lambda: not self.workload.ready(),
                "The action failed because the workload is not ready yet.",
            ),
            (
                lambda: self.get_app_status() != Status.ACTIVE.value,
                "The action failed because the charm is not in active state.",
            ),
            (
                lambda: not self.charm.upgrade_events.idle,
                f"Cannot set password while upgrading (upgrade_stack: {self.charm.upgrade_events.upgrade_stack})",
            ),
        ]

        for check, msg in failure_conditions:
            if check():
                self.logger.error(msg)
                event.set_results({"error": msg})
                event.fail(msg)
                return

        auth = AuthenticationManager(
            cast(DatabaseConnectionInfo, self.context.auth_db), self.context
        )
        password = auth.generate_password()

        if "password" in event.params:
            password = event.params["password"]

        if password == auth.get_password(DEFAULT_ADMIN_USERNAME):
            event.log("The old and new passwords are equal.")
            event.set_results({"password": password})
            return

        auth.set_password(DEFAULT_ADMIN_USERNAME, password)

        event.set_results({"password": password})
