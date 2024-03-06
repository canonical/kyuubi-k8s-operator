#!/usr/bin/env python3
# Copyright 2024 Bikalpa Dhakal
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk

"""Charm the service.

Refer to the following tutorial that will help you
develop a new k8s charm using the Operator Framework:

https://juju.is/docs/sdk/create-a-minimal-kubernetes-charm
"""

import logging
from typing import Optional
from s3 import S3ConnectionInfo
from workload import KyuubiServer
from utils import IOMode
from config import KyuubiServerConfig
import ops
from ops.charm import (
    ActionEvent
)
import json
from charms.data_platform_libs.v0.s3 import (
    CredentialsChangedEvent,
    CredentialsGoneEvent,
    S3Requirer,
)

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)

VALID_LOG_LEVELS = ["info", "debug", "warning", "error", "critical"]


class KyuubiCharm(ops.CharmBase):
    """Charm the service."""
    CONTAINER = "kyuubi"
    S3_INTEGRATOR_REL = "s3-credentials"

    def __init__(self, *args):
        super().__init__(*args)
        self.workload = KyuubiServer(
            self.unit.get_container(self.CONTAINER)
        )
        self.s3_requirer = S3Requirer(self, self.S3_INTEGRATOR_REL)
        self.register_observers()
        self.logger = logger


    def register_observers(self):
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.kyuubi_pebble_ready, self._on_kyuubi_pebble_ready)
        self.framework.observe(self.on.get_jdbc_endpoint_action, self._on_get_jdbc_endpoint)
        self.framework.observe(
            self.s3_requirer.on.credentials_changed, self._on_s3_credential_changed
        )
        self.framework.observe(self.s3_requirer.on.credentials_gone, self._on_s3_credential_gone)

    def _on_start(self, event: ops.StartEvent):
        """Handle start event."""
        self.unit.status = ops.ActiveStatus("Charm started successfully.")

    def _update_spark_configs(self, s3_info: Optional[S3ConnectionInfo] = None):
        with self.workload.get_spark_configuration_file(IOMode.WRITE) as fid:
            spark_config = KyuubiServerConfig(s3_info=s3_info)
            fid.write(spark_config.contents)

    def update_service(
        self,
        s3_info: Optional[S3ConnectionInfo] = None
    ) -> bool:
        """Update the Kyuubi server service if needed."""
        self._update_spark_configs(s3_info=s3_info)
        self.workload.start()
        return True

    def _on_kyuubi_pebble_ready(self, event: ops.PebbleReadyEvent):
        """Define and start a workload using the Pebble API."""
        self.update_service()
        self.unit.status = ops.ActiveStatus("Pebble ready event handled successfully.")

    def _on_get_jdbc_endpoint(self, event: ActionEvent):
        result = {
            "endpoint": self.workload.get_jdbc_endpoint()
        }
        event.set_results(result)

    @property
    def s3_connection_info(self) -> Optional[S3ConnectionInfo]:
        """Parse a S3ConnectionInfo object from relation data."""

        # If the relation is not yet available, do nothing.
        if not self.s3_requirer.relations:
            return None

        raw_info = self.s3_requirer.get_s3_connection_info()

        return S3ConnectionInfo(
            endpoint=raw_info.get("endpoint"),
            access_key=raw_info.get("access-key"),
            secret_key=raw_info.get("secret-key"),
            path=raw_info.get("path"),
            bucket=raw_info.get("bucket"),
        )

    def _on_s3_credential_changed(self, _: CredentialsChangedEvent):
        """Handle the `CredentialsChangedEvent` event from S3 integrator."""
        logger.info("S3 Credentials changed")
        self.update_service(self.s3_connection_info)

        # Checking if the credentials work
        self.s3_connection_info.verify()

    def _on_s3_credential_gone(self, _: CredentialsGoneEvent):
        """Handle the `CredentialsGoneEvent` event for S3 integrator."""
        logger.info("S3 Credentials gone")
        self.update_service()

    # def _on_config_changed(self, event: ops.ConfigChangedEvent):
    #     """Handle changed configuration.

    #     Change this example to suit your needs. If you don't need to handle config, you can remove
    #     this method.

    #     Learn more about config at https://juju.is/docs/sdk/config
    #     """
    #     # Fetch the new config value
    #     log_level = self.model.config["log-level"].lower()

    #     # Do some validation of the configuration option
    #     if log_level in VALID_LOG_LEVELS:
    #         # The config is good, so update the configuration of the workload
    #         container = self.unit.get_container("httpbin")
    #         # Verify that we can connect to the Pebble API in the workload container
    #         if container.can_connect():
    #             # Push an updated layer with the new config
    #             container.add_layer("httpbin", self._pebble_layer, combine=True)
    #             container.replan()

    #             logger.debug("Log level for gunicorn changed to '%s'", log_level)
    #             self.unit.status = ops.ActiveStatus()
    #         else:
    #             # We were unable to connect to the Pebble API, so we defer this event
    #             event.defer()
    #             self.unit.status = ops.WaitingStatus("waiting for Pebble API")
    #     else:
    #         # In this case, the config option is bad, so block the charm and notify the operator.
    #         self.unit.status = ops.BlockedStatus("invalid log level: '{log_level}'")


if __name__ == "__main__":  # pragma: nocover
    ops.main(KyuubiCharm)  # type: ignore
