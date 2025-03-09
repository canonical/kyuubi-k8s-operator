#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Integration Hub related event handlers."""

from ops import CharmBase, RelationChangedEvent

from common.relation.spark_sa import (
    ServiceAccountGoneEvent,
    ServiceAccountGrantedEvent,
    SparkServiceAccountRequirerEventHandlers,
)
from core.context import Context
from core.workload import KyuubiWorkloadBase
from events.base import BaseEventHandler, compute_status, defer_when_not_ready
from managers.kyuubi import KyuubiManager
from utils.logging import WithLogging


class SparkIntegrationHubEvents(BaseEventHandler, WithLogging):
    """Class implementing Integration Hub event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: KyuubiWorkloadBase):
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
        # self.framework.observe(
        #     self.charm.on[SPARK_SERVICE_ACCOUNT_REL].relation_changed,
        #     self._on_spark_properties_changed,
        # )

    @compute_status
    @defer_when_not_ready
    def _on_account_granted(self, _: ServiceAccountGrantedEvent):
        """Handle the `ServiceAccountGrantedEvent` event from integration hub."""
        self.logger.info("Service account received")
        self.kyuubi.update()

    @compute_status
    def _on_account_gone(self, _: ServiceAccountGoneEvent):
        """Handle the `ServiceAccountGoneEvent` event from integration hub."""
        self.logger.info("Service account deleted")
        self.kyuubi.update(set_service_account_none=True)

    @compute_status
    def _on_spark_properties_changed(self, _: RelationChangedEvent):
        """Handle the spark service account relation changed event."""
        self.logger.info("Spark service account properties changed")
        self.kyuubi.update()
