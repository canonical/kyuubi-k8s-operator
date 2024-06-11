#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 Integration related event handlers."""

from charms.data_platform_libs.v0.s3 import (
    CredentialsChangedEvent,
    CredentialsGoneEvent,
    S3Requirer,
)
from common.relation.spark_sa import (
    IntegrationHubRequirer, ServiceAccountGrantedEvent, ServiceAccountGoneEvent
)
from ops import CharmBase

from constants import SPARK_SERVICE_ACCOUNT_REL
from core.domain import ServiceAccountInfo
from core.context import Context
from core.workload import KyuubiWorkloadBase
from events.base import BaseEventHandler, compute_status
from managers.kyuubi import KyuubiManager
from utils.logging import WithLogging


class SparkIntegrationHubEvents(BaseEventHandler, WithLogging):
    """Class implementing S3 Integration event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: KyuubiWorkloadBase):
        super().__init__(charm, "s3")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.workload)
        requested_account = ServiceAccountInfo(charm_config=self.charm.config)

        self.requirer = IntegrationHubRequirer(
            self.charm, SPARK_SERVICE_ACCOUNT_REL,
            requested_account.service_account, requested_account.namespace
        )

        self.framework.observe(
            self.requirer.on.account_granted, self._on_account_granted
        )
        self.framework.observe(
            self.requirer.on.account_gone, self._on_account_gone
        )


    @compute_status
    def _on_account_granted(self, _: ServiceAccountGrantedEvent):
        """Handle the `CredentialsChangedEvent` event from S3 integrator."""
        self.logger.info("Service account received")
        self.kyuubi.update(
            s3_info=self.context.s3,
            metastore_db_info=self.context.metastore_db,
            auth_db_info=self.context.auth_db,
            service_account_info=self.context.service_account,
        )

    @compute_status
    def _on_account_gone(self, _: ServiceAccountGoneEvent):
        """Handle the `CredentialsGoneEvent` event for S3 integrator."""
        self.logger.info("S3 Credentials gone")
        self.logger.info(self.context.s3)
        self.kyuubi.update(
            s3_info=self.context.s3,
            metastore_db_info=self.context.metastore_db,
            auth_db_info=self.context.auth_db,
            service_account_info=None,
        )
