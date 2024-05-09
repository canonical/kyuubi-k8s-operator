#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Base utilities exposing common functionalities for all Events classes."""

from functools import wraps
from typing import Callable

from ops import CharmBase, EventBase, Object, StatusBase

from core.domain import S3ConnectionInfo, ServiceAccountInfo, Status
from managers.s3 import S3Manager
from utils import k8s
from utils.logging import WithLogging
from workload.kyuubi import KyuubiWorkload


class BaseEventHandler(Object, WithLogging):
    """Base class for all Event Handler classes in the Spark Integration Hub."""

    workload: KyuubiWorkload
    charm: CharmBase

    def get_app_status(
        self,
        s3_info: S3ConnectionInfo | None,
        service_account_info: ServiceAccountInfo | None,
    ) -> StatusBase:
        """Return the status of the charm."""
        if not self.workload.ready():
            return Status.WAITING_PEBBLE.value

        if not s3_info:
            return Status.MISSING_S3_RELATION.value

        s3_manager = S3Manager(s3_info=s3_info)
        if not s3_manager.verify():
            return Status.INVALID_CREDENTIALS.value

        namespace = service_account_info.namespace
        if not k8s.is_valid_namespace(namespace=namespace):
            return Status.INVALID_NAMESPACE.value

        service_account = service_account_info.service_account
        if not k8s.is_valid_service_account(namespace=namespace, service_account=service_account):
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
