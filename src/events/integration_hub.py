#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Integration Hub related event handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.spark_service_account import (
    ServiceAccountGoneEvent,
    ServiceAccountGrantedEvent,
    ServiceAccountPropertyChangedEvent,
    SparkServiceAccountRequirerEventHandlers,
)

from core.context import Context
from core.workload.kyuubi import KyuubiWorkload
from events.base import BaseEventHandler, compute_status, defer_when_not_ready
from managers.kyuubi import KyuubiManager
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KyuubiCharm


class SparkIntegrationHubEvents(BaseEventHandler, WithLogging):
    """Class implementing Integration Hub event hooks."""

    def __init__(self, charm: KyuubiCharm, context: Context, workload: KyuubiWorkload) -> None:
        super().__init__(charm, "integration-hub")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.workload, self.context)

        self.service_account_requirer = SparkServiceAccountRequirerEventHandlers(
            self.charm, self.context.spark_service_account_interface
        )

        self.framework.observe(
            self.service_account_requirer.on.account_granted, self._on_account_granted
        )
        self.framework.observe(
            self.service_account_requirer.on.account_gone, self._on_account_gone
        )
        self.framework.observe(
            self.service_account_requirer.on.properties_changed, self._on_spark_properties_changed
        )

    @compute_status
    @defer_when_not_ready
    def _on_account_granted(self, _: ServiceAccountGrantedEvent):
        """Handle the `ServiceAccountGrantedEvent` event from integration hub."""
        self.logger.info("Service account received")
        self.kyuubi.update()

    @compute_status
    @defer_when_not_ready
    def _on_spark_properties_changed(self, _: ServiceAccountPropertyChangedEvent):
        """Handle the spark service account relation changed event."""
        self.logger.info("Service account properties changed")
        self.kyuubi.update()

    @compute_status
    @defer_when_not_ready
    def _on_account_gone(self, _: ServiceAccountGoneEvent):
        """Handle the `ServiceAccountGoneEvent` event from integration hub."""
        self.logger.info("Service account deleted")
        self.kyuubi.update(set_service_account_none=True)
