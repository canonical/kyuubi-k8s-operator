#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Base utilities exposing common functionalities for all Events classes."""

from functools import wraps
from typing import Callable

from ops import CharmBase, EventBase, Object, StatusBase

from core.context import Context
from core.domain import S3ConnectionInfo, SparkServiceAccountInfo, Status
from core.workload import KyuubiWorkloadBase
from managers.k8s import K8sManager
from managers.s3 import S3Manager
from utils.logging import WithLogging


class BaseEventHandler(Object, WithLogging):
    """Base class for all Event Handler classes in the Spark Integration Hub."""

    workload: KyuubiWorkloadBase
    charm: CharmBase
    context: Context

    def get_app_status(
        self,
        s3_info: S3ConnectionInfo | None,
        service_account: SparkServiceAccountInfo | None,
    ) -> StatusBase:
        """Return the status of the charm."""
        if not self.workload.ready():
            return Status.WAITING_PEBBLE.value

        # if not s3_info:
        #     return Status.MISSING_S3_RELATION.value

        if s3_info:
            s3_manager = S3Manager(s3_info=s3_info)
            if not s3_manager.verify():
                return Status.INVALID_CREDENTIALS.value

        if not service_account:
            return Status.MISSING_INTEGRATION_HUB.value

        k8s_manager = K8sManager(service_account_info=service_account, workload=self.workload)

        # Check whether any one of object storage backend has been configured
        # Currently, we do this check on the basis of presence of Spark properties
        # TODO: Rethink on this approach with a more sturdy solution
        if not k8s_manager.is_s3_configured() and not k8s_manager.is_azure_storage_configured():
            return Status.MISSING_OBJECT_STORAGE_BACKEND.value

        if not k8s_manager.is_namespace_valid():
            return Status.INVALID_NAMESPACE.value

        if not k8s_manager.is_service_account_valid():
            return Status.INVALID_SERVICE_ACCOUNT.value

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
            event_handler.charm.app.status = event_handler.get_app_status(
                event_handler.context.s3, event_handler.context.service_account
            )
        event_handler.charm.unit.status = event_handler.get_app_status(
            event_handler.context.s3, event_handler.context.service_account
        )
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
