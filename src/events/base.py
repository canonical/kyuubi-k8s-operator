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
from managers.auth import AuthenticationManager
from managers.hive_metastore import HiveMetastoreManager
from managers.k8s import K8sManager
from managers.service import ServiceManager
from managers.tls import TLSManager
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KyuubiCharm


class BaseEventHandler(Object, WithLogging):
    """Base class for all Event Handler classes in the Spark Integration Hub."""

    workload: KyuubiWorkload
    charm: KyuubiCharm
    context: Context

    def get_app_status(  # noqa: C901
        self,
    ) -> StatusBase:
        """Return the status of the charm."""
        if not self.workload.ready():
            return Status.WAITING_PEBBLE.value

        if not self.context.service_account:
            return Status.MISSING_INTEGRATION_HUB.value

        k8s_manager = K8sManager(
            service_account_info=self.context.service_account, workload=self.workload
        )

        if not k8s_manager.has_cluster_permissions():
            return Status.INSUFFICIENT_CLUSTER_PERMISSIONS.value

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

        auth_manager = AuthenticationManager(self.context)
        if not auth_manager.system_user_secret_configured():
            pass
        elif not auth_manager.system_user_secret_exists():
            return Status.SYSTEM_USERS_SECRET_DOES_NOT_EXIST.value
        elif not auth_manager.system_user_secret_granted():
            return Status.SYSTEM_USERS_SECRET_INSUFFICIENT_PERMISSION.value
        elif not auth_manager.system_user_secret_valid():
            return Status.SYSTEM_USERS_SECRET_INVALID.value

        tls_manager = TLSManager(self.context, self.workload)
        if not tls_manager.tls_private_key_secret_configured():
            pass
        elif not tls_manager.tls_private_key_secret_exists():
            return Status.TLS_SECRET_DOES_NOT_EXIST.value
        elif not tls_manager.tls_private_key_secret_granted():
            return Status.TLS_SECRET_INSUFFICIENT_PERMISSION.value
        elif not tls_manager.tls_private_key_secret_valid():
            return Status.TLS_SECRET_INVALID.value

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

        return Status.ACTIVE.value


def compute_status(
    hook: Callable,
) -> Callable[[BaseEventHandler, EventBase], None]:
    """Decorator to automatically compute statuses at the end of the hook."""

    @wraps(hook)
    def wrapper_hook(event_handler: BaseEventHandler, event: EventBase):
        """Return output after resetting statuses."""
        res = hook(event_handler, event)
        if event_handler.charm.unit.is_leader():
            event_handler.charm.app.status = event_handler.get_app_status()
        event_handler.charm.unit.status = event_handler.get_app_status()
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


def leader_only(
    hook: Callable,
) -> Callable[[BaseEventHandler, EventBase], None]:
    """Run the hook only on leader unit."""

    @wraps(hook)
    def wrapper_hook(event_handler: BaseEventHandler, event: EventBase):
        """Return none when not leader, proceed with normal hook otherwise."""
        if not event_handler.charm.unit.is_leader():
            return

        return hook(event_handler, event)

    return wrapper_hook
