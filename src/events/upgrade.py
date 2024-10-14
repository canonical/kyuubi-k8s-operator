#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Refresh related event handlers."""

from charms.data_platform_libs.v0.upgrade import (
    ClusterNotReadyError,
    DataUpgrade,
    DependencyModel,
    EventBase,
    KubernetesClientError,
)
from lightkube.core.client import Client
from lightkube.core.exceptions import ApiError
from lightkube.resources.apps_v1 import StatefulSet
from ops import CharmBase, ModelError
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_random
from typing_extensions import override

from constants import KYUUBI_CONTAINER_NAME
from core.context import Context
from core.workload import KyuubiWorkloadBase
from events.base import BaseEventHandler
from managers.kyuubi import KyuubiManager


class KyuubiDependencyModel(BaseModel):
    """Model for Kyuubi Operator dependencies."""

    service: DependencyModel


class UpgradeEvents(DataUpgrade, BaseEventHandler):
    """Class implementing Upgrade integration event hooks."""

    def __init__(
        self,
        charm: CharmBase,
        context: Context,
        workload: KyuubiWorkloadBase,
        dependency_model: BaseModel,
    ):
        super().__init__(charm, dependency_model=dependency_model, substrate="k8s")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.workload)
        self.framework.observe(
            getattr(self.charm.on, "upgrade_charm"), self._on_kyuubi_pebble_ready_upgrade
        )

    def _on_kyuubi_pebble_ready_upgrade(self, _: EventBase):
        """Handler for the `upgrade-charm` events during the in-place upgrades."""
        self.logger.info("Kyuubi upgrade...")

        # ensure pebble-ready only fires after normal peer-relation-driven server init
        if not self.workload.ready():
            self.logger.info("Workload is not ready")
            return

        # useful to have a log here to indicate that the upgrade is progressing
        try:
            # check if workload is active
            self.workload.active()
        except ModelError:
            self.logger.info(
                f"{KYUUBI_CONTAINER_NAME} workload service not running, re-initialising..."
            )

        # re-initialise + replan pebble layer if no service, or service not running
        self.kyuubi.update(
            s3_info=self.context.s3,
            metastore_db_info=self.context.metastore_db,
            auth_db_info=self.context.auth_db,
            service_account_info=self.context.service_account,
            zookeeper_info=self.context.zookeeper,
        )

        # check if upgrade is successful and set unit upgrade status
        try:
            self.post_upgrade_check()
        except Exception as e:
            self.logger.error(e)
            self.set_unit_failed()
            return

        self.set_unit_completed()
        self.logger.info("Upgrade completed.")

    @retry(stop=stop_after_attempt(5), wait=wait_random(min=1, max=5), reraise=True)
    def post_upgrade_check(self) -> None:
        """Runs necessary checks validating the unit is in a healthy state after upgrade."""
        self.logger.info("Post upgrade checks")
        self.pre_upgrade_check()

    @override
    def pre_upgrade_check(self) -> None:
        """Pre-upgrade-tests function."""
        self.logger.info("Post upgrade check")
        default_message = "Pre-upgrade check failed and cannot safely upgrade"

        if not self.workload.active():
            raise ClusterNotReadyError(message=default_message, cause="Cluster is not healthy")

    @override
    def log_rollback_instructions(self) -> None:
        """Return rollback instructions."""
        self.logger.critical(
            "\n".join(
                [
                    "Unit failed to refresh and requires manual rollback to previous stable version.",
                    "    1. Re-run `pre-upgrade-check` action on the leader unit to enter 'recovery' state",
                    "    2. Run `juju refresh` to the previously deployed charm revision",
                ]
            )
        )
        return

    @override
    def _set_rolling_update_partition(self, partition: int) -> None:
        """Set the rolling update partition to a specific value."""
        try:
            patch = {"spec": {"updateStrategy": {"rollingUpdate": {"partition": partition}}}}
            Client().patch(  # pyright: ignore [reportArgumentType]
                StatefulSet,
                name=self.charm.model.app.name,
                namespace=self.charm.model.name,
                obj=patch,
            )
            self.logger.debug(f"Kubernetes StatefulSet partition set to {partition}")
        except ApiError as e:
            if e.status.code == 403:
                cause = "`juju trust` needed"
            else:
                cause = str(e)
            raise KubernetesClientError("Kubernetes StatefulSet patch failed", cause)
