#!/usr/bin/env -S LD_LIBRARY_PATH=lib python3
# The LD_LIBRARY_PATH variable needs to be set here because without that
# psycopg2 can't be imported due to missing libpq.so file (which is inside lib/)

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charm the Kyuubi service."""

from __future__ import annotations

import logging

import charm_refresh
import ops
from charms.data_platform_libs.v0.data_models import TypedCharmBase
from charms.grafana_k8s.v0.grafana_dashboard import GrafanaDashboardProvider
from charms.loki_k8s.v1.loki_push_api import LogForwarder
from charms.prometheus_k8s.v0.prometheus_scrape import MetricsEndpointProvider

from constants import (
    COS_LOG_RELATION_NAME_SERVER,
    COS_METRICS_PATH,
    COS_METRICS_PORT,
    KYUUBI_CONTAINER_NAME,
)
from core.config import CharmConfig
from core.context import Context
from core.workload.kyuubi import KyuubiWorkload
from events.actions import ActionEvents
from events.auth import AuthenticationEvents
from events.integration_hub import SparkIntegrationHubEvents
from events.kyuubi import KyuubiEvents
from events.metastore import MetastoreEvents
from events.provider import KyuubiClientProviderEvents
from events.refresh import KyuubiRefresh
from events.tls import TLSEvents
from events.zookeeper import ZookeeperEvents

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class KyuubiCharm(TypedCharmBase[CharmConfig]):
    """Charm the service."""

    config_type = CharmConfig

    def __init__(self, *args) -> None:
        super().__init__(*args)

        # Workload
        self.workload = KyuubiWorkload(
            container=self.unit.get_container(KYUUBI_CONTAINER_NAME),
        )

        # Context
        self.context = Context(model=self.model, config=self.config)

        # Event handlers
        self.kyuubi_events = KyuubiEvents(self, self.context, self.workload)
        self.hub_events = SparkIntegrationHubEvents(self, self.context, self.workload)
        self.metastore_events = MetastoreEvents(self, self.context, self.workload)
        self.auth_events = AuthenticationEvents(self, self.context, self.workload)
        self.zookeeper_events = ZookeeperEvents(self, self.context, self.workload)
        self.action_events = ActionEvents(self, self.context, self.workload)
        self.tls_events = TLSEvents(self, self.context, self.workload)
        self.provider_events = KyuubiClientProviderEvents(self, self.context, self.workload)

        # Monitoring/alerting (COS)
        # Prometheus
        self.metrics_endpoint = MetricsEndpointProvider(
            self,
            refresh_event=self.on.start,
            jobs=[
                {
                    "metrics_path": COS_METRICS_PATH,
                    "static_configs": [{"targets": [f"*:{COS_METRICS_PORT}"]}],
                }
            ],
        )
        # Grafana Dashboards
        self.grafana_dashboards = GrafanaDashboardProvider(self)

        # Loki
        # Server logs from Pebble
        self._log_forwarder = LogForwarder(self, relation_name=COS_LOG_RELATION_NAME_SERVER)

        self.refresh: charm_refresh.Kubernetes | None
        try:
            self.refresh = charm_refresh.Kubernetes(
                KyuubiRefresh(
                    workload_name="Kyuubi",
                    charm_name="kyuubi-k8",
                    oci_resource_name="kyuubi-image",
                    _charm=self,
                )
            )
        except (charm_refresh.UnitTearingDown, charm_refresh.PeerRelationNotReady):
            self.refresh = None

        if (
            self.refresh is not None
            and not self.refresh.next_unit_allowed_to_refresh
            and self.refresh.workload_allowed_to_start
        ):
            self.refresh.next_unit_allowed_to_refresh = True

        # Reconcile app and unit status
        self.on.update_status.emit()


if __name__ == "__main__":  # pragma: nocover
    ops.main(KyuubiCharm)
