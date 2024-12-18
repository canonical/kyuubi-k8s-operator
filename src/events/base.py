#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Base utilities exposing common functionalities for all Events classes."""

from functools import wraps
from typing import Callable

from charms.data_platform_libs.v0.data_models import TypedCharmBase
from ops import EventBase, Object, StatusBase

from core.context import Context
from core.domain import Status
from core.workload import KyuubiWorkloadBase
from managers.k8s import K8sManager
from managers.s3 import S3Manager
from managers.service import ServiceManager
from utils.logging import WithLogging


class BaseEventHandler(Object, WithLogging):
    """Base class for all Event Handler classes in the Spark Integration Hub."""

    workload: KyuubiWorkloadBase
    charm: TypedCharmBase
    context: Context

    def get_app_status(  # noqa: C901
        self,
    ) -> StatusBase:
        """Return the status of the charm."""
        if not self.workload.ready():
            return Status.WAITING_PEBBLE.value

        if self.context.s3:
            s3_manager = S3Manager(s3_info=self.context.s3)
            if not s3_manager.verify():
                return Status.INVALID_CREDENTIALS.value

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
        if (
            not self.context.s3
            and not k8s_manager.is_s3_configured()
            and not k8s_manager.is_azure_storage_configured()
        ):
            return Status.MISSING_OBJECT_STORAGE_BACKEND.value

        if not k8s_manager.is_namespace_valid():
            return Status.INVALID_NAMESPACE.value

        if not k8s_manager.is_service_account_valid():
            return Status.INVALID_SERVICE_ACCOUNT.value

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
    hook: Callable[[BaseEventHandler, EventBase], None]
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
    hook: Callable[[BaseEventHandler, EventBase], None]
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
