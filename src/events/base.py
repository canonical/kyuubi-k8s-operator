#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Base utilities exposing common functionalities for all Events classes."""

from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Callable

from ops import EventBase, Object, StatusBase

from core.context import Context
from core.domain import Status
from core.workload.kyuubi import KyuubiWorkload
from managers.hive_metastore import HiveMetastoreManager
from managers.k8s import K8sManager
from managers.service import ServiceManager
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KyuubiCharm


class BaseEventHandler(Object, WithLogging):
    """Base class for all Event Handler classes in the Spark Integration Hub."""

    workload: KyuubiWorkload
    charm: KyuubiCharm
    context: Context

    def get_app_status(  # noqa: C901 - ignore complexity threshold
        self, check_refresh: bool = False
    ) -> StatusBase:
        """Return the status of the charm."""
        if (
            check_refresh
            and self.charm.refresh is not None
            and (refresh_status := self.charm.refresh.unit_status_higher_priority) is not None
        ):
            # If we have a high priority refresh status for the unit, then we must display
            # it before anything else.
            return refresh_status

        if not self.workload.ready():
            return Status.WAITING_PEBBLE.value

        if not self.context.service_account:
            return Status.MISSING_INTEGRATION_HUB.value

        k8s_manager = K8sManager(
            service_account_info=self.context.service_account,
            workload=self.workload,
        )

        # Check whether any one of object storage backend has been configured
        # Currently, we do this check on the basis of presence of Spark properties
        # TODO: Rethink on this approach with a more sturdy solution
        if not k8s_manager.is_s3_configured() and not k8s_manager.is_azure_storage_configured():
            return Status.MISSING_OBJECT_STORAGE_BACKEND.value

        if not k8s_manager.is_namespace_valid():
            return Status.INVALID_NAMESPACE.value

        if not k8s_manager.is_service_account_valid():
            return Status.INVALID_SERVICE_ACCOUNT.value

        if not self.context.auth_db:
            return Status.MISSING_AUTH_DB.value

        metastore_manager = HiveMetastoreManager(self.workload)
        if self.context.metastore_db and not metastore_manager.is_metastore_valid():
            return Status.INVALID_METASTORE_SCHEMA.value

        if self.context._zookeeper_relation and not self.context.zookeeper:
            return Status.WAITING_ZOOKEEPER.value

        if self.charm.app.planned_units() > 1 and not self.context.zookeeper:
            return Status.MISSING_ZOOKEEPER.value

        service_manager = ServiceManager(
            namespace=self.charm.model.name,
            unit_name=self.charm.unit.name,
            app_name=self.charm.app.name,
        )

        if not service_manager.get_service_endpoint(
            expose_external=self.charm.config.expose_external
        ):
            return Status.WAITING_FOR_SERVICE.value

        if (
            check_refresh
            and self.charm.refresh is not None
            and (refresh_status := self.charm.refresh.unit_status_lower_priority()) is not None
        ):
            # If we have a low priority refresh status for the unit, then we can display
            # it after all the others requiring domain logic.
            return refresh_status

        return Status.ACTIVE.value


def compute_status(
    hook: Callable,
) -> Callable[[BaseEventHandler, EventBase], None]:
    """Decorator to automatically compute statuses at the end of the hook."""

    @wraps(hook)
    def wrapper_hook(event_handler: BaseEventHandler, event: EventBase):
        """Return output after resetting statuses.

        We have 4 kinds of statuses in the charm: domain-logic ones, refresh app, refresh
        unit high priority and refresh unit low priority.
        Refresh app status, if set, must be displayed before domain-logic one. Same for
        refresh unit high priority.
        """
        res = hook(event_handler, event)
        if event_handler.charm.unit.is_leader():
            if (refresh := event_handler.charm.refresh) is not None and (
                refresh_app_status := refresh.app_status_higher_priority
            ) is not None:
                event_handler.charm.app.status = refresh_app_status
            else:
                event_handler.charm.app.status = event_handler.get_app_status()
        event_handler.charm.unit.status = event_handler.get_app_status(check_refresh=True)
        return res

    return wrapper_hook


def defer_when_not_ready(
    hook: Callable,
) -> Callable[[BaseEventHandler, EventBase], None]:
    """Decorator to defer hook is workload is not ready."""

    @wraps(hook)
    def wrapper_hook(event_handler: BaseEventHandler, event: EventBase):
        """Return none when not ready, proceed with normal hook otherwise."""
        if not event_handler.workload.ready():
            event.defer()
            return None

        return hook(event_handler, event)

    return wrapper_hook
