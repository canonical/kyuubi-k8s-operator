#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 Integration related event handlers."""

from charms.data_platform_libs.v0.s3 import (
    CredentialsChangedEvent,
    CredentialsGoneEvent,
)
from ops import CharmBase

from core.context import Context
from events.base import BaseEventHandler, compute_status
from managers.kyuubi import KyuubiManager
from utils.logging import WithLogging
from workload.base import KyuubiWorkloadBase


class S3Events(BaseEventHandler, WithLogging):
    """Class implementing S3 Integration event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: KyuubiWorkloadBase):
        super().__init__(charm, "s3")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.workload)

        self.framework.observe(
            self.charm.s3.on.credentials_changed, self._on_s3_credential_changed
        )
        self.framework.observe(self.charm.s3.on.credentials_gone, self._on_s3_credential_gone)

    @compute_status
    def _on_s3_credential_changed(self, _: CredentialsChangedEvent):
        """Handle the `CredentialsChangedEvent` event from S3 integrator."""
        self.logger.info("S3 Credentials changed")
        self.kyuubi.update(
            s3_info=self.context.s3,
            metastore_db_info=self.context.metastore_db,
            auth_db_info=self.context.auth_db,
            service_account_info=self.context.service_account,
        )

    @compute_status
    def _on_s3_credential_gone(self, _: CredentialsGoneEvent):
        """Handle the `CredentialsGoneEvent` event for S3 integrator."""
        self.logger.info("S3 Credentials gone")
        self.kyuubi.update(
            s3_info=self.context.s3,
            metastore_db_info=self.context.metastore_db,
            auth_db_info=self.context.auth_db,
            service_account_info=self.context.service_account,
        )
