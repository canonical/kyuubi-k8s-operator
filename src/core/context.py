#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charm Context definition and parsing logic."""


# from charms.data_platform_libs.v0.data_interfaces import RequirerData
from ops import CharmBase, Relation

from constants import S3_INTEGRATOR_REL
from core.domain import DatabaseConnectionInfo, S3ConnectionInfo, ServiceAccountInfo
from utils.logging import WithLogging


class Context(WithLogging):
    """Properties and relations of the charm."""

    def __init__(self, charm: CharmBase):

        self.charm = charm
        self.model = charm.model

    @property
    def _s3_relation(self) -> Relation | None:
        """The S3 relation."""
        return self.charm.model.get_relation(S3_INTEGRATOR_REL)

    # --- DOMAIN OBJECTS ---

    @property
    def s3(self) -> S3ConnectionInfo | None:
        """The state of S3 connection."""
        return S3ConnectionInfo(rel, rel.app) if (rel := self._s3_relation) else None

    @property
    def metastore_db(self):
        """The state of metastore DB connection."""
        for data in self.charm.metastore_db.fetch_relation_data().values():
            if any(key not in data for key in ["endpoints", "username", "password"]):
                continue
            return DatabaseConnectionInfo(
                endpoint=data["endpoints"],
                username=data["username"],
                password=data["password"],
                dbname=data["database"],
                # dbname=METASTORE_DATABASE_NAME,
            )
        return None

    @property
    def auth_db(self):
        """The state of authentication DB connection."""
        for data in self.charm.auth_db.fetch_relation_data().values():
            if any(key not in data for key in ["endpoints", "username", "password"]):
                continue
            return DatabaseConnectionInfo(
                endpoint=data["endpoints"],
                username=data["username"],
                password=data["password"],
                dbname=data["database"],
                # dbname=AUTHENTICATION_DATABASE_NAME,
            )
        return None

    @property
    def service_account(self):
        """The state of service account information."""
        return ServiceAccountInfo(self.charm)

    def is_authentication_enabled(self) -> bool:
        """Returns whether the authentication has been enabled in the Kyuubi charm."""
        return bool(self.auth_db)
