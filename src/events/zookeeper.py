#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Zookeeper related event handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.data_interfaces import DatabaseRequirerEventHandlers
from ops.charm import RelationBrokenEvent, RelationChangedEvent

from constants import ZOOKEEPER_REL
from core.context import Context
from core.workload.kyuubi import KyuubiWorkload
from events.base import BaseEventHandler, defer_when_not_ready
from managers.kyuubi import KyuubiManager
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KyuubiCharm


class ZookeeperEvents(BaseEventHandler, WithLogging):
    """Class implementing Zookeeper integration event hooks."""

    def __init__(self, charm: KyuubiCharm, context: Context, workload: KyuubiWorkload) -> None:
        super().__init__(charm, "zookeeper")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.charm, self.workload, self.context)
        self.zookeeper_handler = DatabaseRequirerEventHandlers(
            self.charm, self.context.zookeeper_requirer_data
        )

        self.framework.observe(
            self.charm.on[ZOOKEEPER_REL].relation_changed, self._on_zookeeper_changed
        )
        self.framework.observe(
            self.charm.on[ZOOKEEPER_REL].relation_broken, self._on_zookeeper_broken
        )

    @defer_when_not_ready
    def _on_zookeeper_changed(self, _: RelationChangedEvent):
        self.logger.info("Zookeeper relation changed new...")
        self.kyuubi.update()

    @defer_when_not_ready
    def _on_zookeeper_broken(self, _: RelationBrokenEvent):
        self.logger.info("Zookeeper relation broken...")
        self.kyuubi.update(set_zookeeper_none=True)
