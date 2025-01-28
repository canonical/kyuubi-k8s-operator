#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Integration Hub related event handlers."""

from ops import CharmBase

from common.relation.spark_sa import (
    IntegrationHubRequirer,
    ServiceAccountGoneEvent,
    ServiceAccountGrantedEvent,
)
from constants import (
    SPARK_SERVICE_ACCOUNT_REL,
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

        namespace = self.charm.config.namespace
        service_account = self.charm.config.service_account

        self.requirer = IntegrationHubRequirer(
            self.charm,
            SPARK_SERVICE_ACCOUNT_REL,
            service_account,
            namespace if namespace else self.model.name,
        )

        self.framework.observe(self.requirer.on.account_granted, self._on_account_granted)
        self.framework.observe(self.requirer.on.account_gone, self._on_account_gone)

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
