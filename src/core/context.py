#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charm Context definition and parsing logic."""

from charms.data_platform_libs.v0.data_interfaces import DatabaseRequirerData
from ops import ConfigData, Model, Relation

from common.relation.spark_sa import RequirerData
from constants import (
    AUTHENTICATION_DATABASE_NAME,
    METASTORE_DATABASE_NAME,
    POSTGRESQL_AUTH_DB_REL,
    POSTGRESQL_METASTORE_DB_REL,
    S3_INTEGRATOR_REL,
    SPARK_SERVICE_ACCOUNT_REL,
    ZOOKEEPER_REL
)
from core.domain import DatabaseConnectionInfo, S3ConnectionInfo, SparkServiceAccountInfo, ZookeeperInfo
from utils.logging import WithLogging


class Context(WithLogging):
    """Properties and relations of the charm."""

    def __init__(self, model: Model, config: ConfigData):
        self.model = model
        self.charm_config = config
        self.metastore_db_requirer = DatabaseRequirerData(
            self.model, POSTGRESQL_METASTORE_DB_REL, database_name=METASTORE_DATABASE_NAME
        )
        self.auth_db_requirer = DatabaseRequirerData(
            self.model,
            POSTGRESQL_AUTH_DB_REL, 
            database_name=AUTHENTICATION_DATABASE_NAME,
            extra_user_roles="superuser",
        )
        self.zookeeper_requirer_data = DatabaseRequirerData(
            self.model, ZOOKEEPER_REL, database_name="kyuubist"
        )

    @property
    def _s3_relation(self) -> Relation | None:
        """The S3 relation."""
        return self.model.get_relation(S3_INTEGRATOR_REL)

    @property
    def _spark_account_relation(self) -> Relation | None:
        """The integration hub relation."""
        return self.model.get_relation(SPARK_SERVICE_ACCOUNT_REL)

    @property
    def _zookeeper_relation(self) -> Relation | None:
        """The zookeeper relation"""
        return self.model.get_relation(ZOOKEEPER_REL)

    # --- DOMAIN OBJECTS ---

    @property
    def s3(self) -> S3ConnectionInfo | None:
        """The state of S3 connection."""
        return S3ConnectionInfo(rel, rel.app) if (rel := self._s3_relation) else None

    @property
    def metastore_db(self) -> DatabaseConnectionInfo | None:
        """The state of metastore DB connection."""
        for data in self.metastore_db_requirer.fetch_relation_data().values():
            if any(key not in data for key in ["endpoints", "username", "password"]):
                continue
            return DatabaseConnectionInfo(
                endpoint=data["endpoints"],
                username=data["username"],
                password=data["password"],
                dbname=data["database"],
            )
        return None

    @property
    def auth_db(self) -> DatabaseConnectionInfo | None:
        """The state of authentication DB connection."""
        for data in self.auth_db_requirer.fetch_relation_data().values():
            if any(key not in data for key in ["endpoints", "username", "password"]):
                continue
            return DatabaseConnectionInfo(
                endpoint=data["endpoints"],
                username=data["username"],
                password=data["password"],
                dbname=data["database"],
            )
        return None

    @property
    def service_account(self) -> SparkServiceAccountInfo | None:
        """The state of service account information."""
        data_interface = RequirerData(self.model, SPARK_SERVICE_ACCOUNT_REL)

        if account := SparkServiceAccountInfo(
            self._spark_account_relation, data_interface, self.model.app
        ):
            return account

    @property
    def zookeeper(self) -> ZookeeperInfo | None:
        """The state of the Zookeeper information."""
        return ZookeeperInfo(rel, self.zookeeper_requirer_data, rel.app) if (rel := self._zookeeper_relation) else None


    def is_authentication_enabled(self) -> bool:
        """Returns whether the authentication has been enabled in the Kyuubi charm."""
        return bool(self.auth_db)
