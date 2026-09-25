#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

"""Service Mesh Integration related event handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from canonical_service_mesh.k8s.resource_manager import (
    KubernetesResourceManager,
    PolicyResourceManager,
)
from canonical_service_mesh.k8s.types.istio import AuthorizationPolicy
from canonical_service_mesh.models.istio import (
    AuthorizationPolicySpec,
    Operation,
    Rule,
    To,
    WorkloadSelector,
)
from charmlibs.interfaces.service_mesh import (
    ServiceMeshConsumer,
    UnitPolicy,
)
from lightkube import Client
from lightkube.models.meta_v1 import ObjectMeta

from constants import (
    COS_METRICS_PORT,
    JDBC_PORT,
    METRICS_RELATION_NAME,
    SERVICE_MESH_RELATION_NAME,
)
from core.context import Context
from core.domain import PeerAuthentication
from core.workload.kyuubi import KyuubiWorkload
from events.base import BaseEventHandler
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KyuubiCharm


class ServiceMeshEvents(BaseEventHandler, WithLogging):
    """Class implementing Ambient Service Mesh event hooks."""

    def __init__(self, charm: KyuubiCharm, context: Context, workload: KyuubiWorkload):
        super().__init__(charm, "service-mesh")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.service_mesh = ServiceMeshConsumer(
            self.charm,
            mesh_relation_name=SERVICE_MESH_RELATION_NAME,
            policies=[UnitPolicy(relation=METRICS_RELATION_NAME, ports=[COS_METRICS_PORT])],
        )
        self.framework.observe(
            self.charm.on[SERVICE_MESH_RELATION_NAME].relation_created,
            self._on_service_mesh_joined,
        )
        self.framework.observe(
            self.charm.on[SERVICE_MESH_RELATION_NAME].relation_broken, self._on_service_mesh_broken
        )

    def _get_jdbc_authorization_policy(self) -> dict:
        """Generate raw Istio AuthorizationPolicy dict."""
        return AuthorizationPolicy(
            metadata=ObjectMeta(
                name=f"{self.charm.app.name}-allow-external-jdbc",
                namespace=self.model.name,
            ),
            spec=AuthorizationPolicySpec(
                rules=[Rule(to=[To(operation=Operation(ports=[str(JDBC_PORT)]))])],
                selector=WorkloadSelector(
                    matchLabels={"app.kubernetes.io/name": self.charm.app.name}
                ),
            ).model_dump(by_alias=True, exclude_unset=True, exclude_none=True),
        )

    def _get_peer_authentication(self):
        """Downgrade mTLS to PERMISSIVE on JDBC port for external LoadBalancer access."""
        return PeerAuthentication(
            metadata=ObjectMeta(
                name=f"{self.charm.app.name}-jdbc-permissive",
                namespace=self.charm.model.name,
            ),
            spec={
                "selector": {"matchLabels": {"app.kubernetes.io/name": self.charm.app.name}},
                "mtls": {"mode": "STRICT"},
                "portLevelMtls": {JDBC_PORT: {"mode": "PERMISSIVE"}},
            },
        )

    def _get_policy_manager(self) -> PolicyResourceManager:
        """Instantiate PolicyResourceManager with unique ownership labels."""
        return PolicyResourceManager(
            charm=self.charm,
            lightkube_client=Client(
                field_manager=f"{self.charm.app.name}-{self.charm.model.name}"
            ),
            labels={
                "app.kubernetes.io/instance": f"{self.charm.app.name}-{self.charm.model.name}",
                "kubernetes-resource-handler-scope": f"{self.charm.app.name}-jdbc-policy",
            },
            logger=self.logger,
        )

    def _get_peer_auth_manager(self) -> KubernetesResourceManager:
        return KubernetesResourceManager(
            lightkube_client=Client(
                field_manager=f"{self.charm.app.name}-{self.charm.model.name}"
            ),
            labels={
                "app.kubernetes.io/instance": f"{self.charm.app.name}-{self.charm.model.name}",
                "kubernetes-resource-handler-scope": f"{self.charm.app.name}-jdbc-peer-authentication",
            },
            logger=self.logger,
            resource_types={PeerAuthentication},
        )

    def _on_service_mesh_joined(self, event) -> None:
        self.reconcile_mesh_resources()

    def _on_service_mesh_broken(self, event) -> None:
        self.delete_mesh_resources()

    def reconcile_mesh_resources(self):
        """Reconcile the AuthorizationPolicy and PeerAuthentication resources."""
        if not self.charm.unit.is_leader():
            return

        mesh_type = self.service_mesh.mesh_type()
        if not mesh_type:
            self.logger.info("No active service mesh connection, skipping policy reconciliation")
            return

        peer_auth_manager = self._get_peer_auth_manager()
        policy_manager = self._get_policy_manager()
        peer_auth_manager.reconcile(
            resources=[
                self._get_peer_authentication(),
            ],
        )
        policy_manager.reconcile(
            [], mesh_type=mesh_type, raw_policies=[self._get_jdbc_authorization_policy()]
        )

    def delete_mesh_resources(self):
        """Delete AuthorizationPolicy and PeerAuthentication resources created by this charm."""
        if not self.charm.unit.is_leader():
            return

        policy_manager = self._get_policy_manager()
        peer_auth_manager = self._get_peer_auth_manager()
        policy_manager.delete()
        peer_auth_manager.delete()
