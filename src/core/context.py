#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charm Context definition and parsing logic."""

from enum import Enum

# from charms.data_platform_libs.v0.data_interfaces import RequirerData
from ops import ActiveStatus, BlockedStatus, CharmBase, MaintenanceStatus, Relation

from utils.logging import WithLogging
from constants import S3_INTEGRATOR_REL
from core.domain import S3ConnectionInfo, DatabaseConnectionInfo, ServiceAccountInfo


class Context(WithLogging):
    """Properties and relations of the charm."""

    def __init__(self, charm: CharmBase):

        self.charm = charm
        self.model = charm.model

    @property
    def _s3_relation_id(self) -> int | None:
        """The S3 relation."""
        return relation.id if (relation := self.charm.model.get_relation(S3_INTEGRATOR_REL)) else None

    @property
    def _s3_relation(self) -> Relation | None:
        """The S3 relation."""
        return self.charm.model.get_relation(S3_INTEGRATOR_REL)

    # --- DOMAIN OBJECTS ---

    @property
    def s3(self) -> S3ConnectionInfo | None:
        """The server state of the current running Unit."""
        return S3ConnectionInfo(rel, rel.app) if (rel := self._s3_relation) else None

    @property
    def metastore_db(self):
        pass

    @property
    def auth_db(self):
        pass

    @property
    def service_account(self):
        return ServiceAccountInfo(self.charm)

    def is_authentication_enabled(self) -> bool:
        """Returns whether the authentication has been enabled in the Kyuubi charm."""
        return bool(self.auth_db)
