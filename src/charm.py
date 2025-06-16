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
from ops.log import JujuLogHandler

from constants import (
    COS_LOG_RELATION_NAME_SERVER,
    COS_METRICS_PATH,
    COS_METRICS_PORT,
    KYUUBI_CONTAINER_NAME,
)
from core.config import CharmConfig
from core.context import Context
from core.domain import Status
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
from managers.hive_metastore import HiveMetastoreManager
from managers.k8s import K8sManager
from managers.service import ServiceManager

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


class KyuubiCharm(TypedCharmBase[CharmConfig]):
    """Charm the service."""

    config_type = CharmConfig

    def __init__(self, *args) -> None:
        super().__init__(*args)
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            if isinstance(handler, JujuLogHandler):
                handler.setFormatter(logging.Formatter("{name}:{message}", style="{"))

        self.refresh: charm_refresh.Kubernetes | None = None
        # Workload
        self.workload = KyuubiWorkload(
            container=self.unit.get_container(KYUUBI_CONTAINER_NAME),
        )

        # Context
        self.context = Context(model=self.model, config=self.config)

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
            if self.workload.active():
                self.refresh.next_unit_allowed_to_refresh = True

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

        self.framework.observe(self.on.collect_unit_status, self._on_collect_unit_status)
        self.framework.observe(self.on.collect_app_status, self._on_collect_app_status)

    def _on_collect_unit_status(self, event: ops.CollectStatusEvent) -> None:
        """Set the status of the unit.

        This must be the only place in the codebase where we set the unit status.

        The priority order is as follows:
        - refresh v3 high priority status
        - domain logic
        - refresh v3 low priority status
        - plain active status
        """
        if (
            self.refresh is not None
            and (refresh_status := self.refresh.unit_status_higher_priority) is not None
        ):
            event.add_status(refresh_status)
            return

        for status in self._collect_domain_statuses():
            event.add_status(status)

        if (
            self.refresh is not None
            and (
                refresh_status := self.refresh.unit_status_lower_priority(
                    workload_is_running=self.workload.active()
                )
            )
            is not None
        ):
            event.add_status(refresh_status)

        event.add_status(Status.ACTIVE.value)

    def _on_collect_app_status(self, event: ops.CollectStatusEvent) -> None:
        """Set the status of the app.

        This must be the only place in the codebase where we set the app status.

        If we have a refresh v3 app status, then we must display it before anything else.
        """
        if (
            self.refresh is not None
            and (refresh_app_status := self.refresh.app_status_higher_priority) is not None
        ):
            event.add_status(refresh_app_status)
            return

        for status in self._collect_domain_statuses():
            event.add_status(status)

        event.add_status(Status.ACTIVE.value)

    def _collect_domain_statuses(self) -> list[ops.StatusBase]:  # noqa: C901 - ignore complexity threshold
        """Status of the charm."""
        statuses: list[ops.StatusBase] = []
        if not self.workload.ready():
            statuses.append(Status.WAITING_PEBBLE.value)
            return statuses

        if not self.context.service_account:
            statuses.append(Status.MISSING_INTEGRATION_HUB.value)
            # Early return, we need the service account for the next bits
            return statuses

        k8s_manager = K8sManager(
            service_account_info=self.context.service_account,
            workload=self.workload,
        )

        # Check whether any one of object storage backend has been configured
        # Currently, we do this check on the basis of presence of Spark properties
        # TODO: Rethink on this approach with a more sturdy solution
        if not k8s_manager.is_s3_configured() and not k8s_manager.is_azure_storage_configured():
            statuses.append(Status.MISSING_OBJECT_STORAGE_BACKEND.value)

        if not k8s_manager.is_namespace_valid():
            statuses.append(Status.INVALID_NAMESPACE.value)

        if not k8s_manager.is_service_account_valid():
            statuses.append(Status.INVALID_SERVICE_ACCOUNT.value)

        if not self.context.auth_db:
            statuses.append(Status.MISSING_AUTH_DB.value)

        metastore_manager = HiveMetastoreManager(self.workload)
        if self.context.metastore_db and not metastore_manager.is_metastore_valid():
            statuses.append(Status.INVALID_METASTORE_SCHEMA.value)

        if self.context._zookeeper_relation and not self.context.zookeeper:
            statuses.append(Status.WAITING_ZOOKEEPER.value)

        if self.app.planned_units() > 1 and not self.context.zookeeper:
            statuses.append(Status.MISSING_ZOOKEEPER.value)

        service_manager = ServiceManager(
            namespace=self.model.name,
            unit_name=self.unit.name,
            app_name=self.app.name,
        )

        if not service_manager.get_service_endpoint(expose_external=self.config.expose_external):
            statuses.append(Status.WAITING_FOR_SERVICE.value)

        return statuses


if __name__ == "__main__":  # pragma: nocover
    ops.main(KyuubiCharm)
