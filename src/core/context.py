#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charm Context definition and parsing logic."""

from ipaddress import IPv4Address, IPv6Address

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseRequirerData,
    DataPeerData,
    DataPeerUnitData,
)
from charms.glauth_k8s.v0.ldap import (
    LdapRequirer,
)
from charms.spark_integration_hub_k8s.v0.spark_service_account import (
    SparkServiceAccountRequirerData,
)
from ops import CharmBase, Relation
from ops.model import Unit

from constants import (
    AUTHENTICATION_DATABASE_NAME,
    CERTIFICATES_TRANSFER_RELATION_NAME,
    HA_ZNODE_NAME,
    KYUUBI_CLIENT_RELATION_NAME,
    LDAP_RELATION_NAME,
    METASTORE_DATABASE_NAME,
    PEER_REL,
    POSTGRESQL_AUTH_DB_REL,
    POSTGRESQL_METASTORE_DB_REL,
    SECRETS_APP,
    SPARK_SERVICE_ACCOUNT_REL,
    TLS_REL,
    ZOOKEEPER_REL,
)
from core.config import CharmConfig
from core.domain import (
    DatabaseConnectionInfo,
    KyuubiCluster,
    KyuubiServer,
    LDAPInfo,
    SparkServiceAccountInfo,
    TLSInfo,
    ZookeeperInfo,
)
from utils.logging import WithLogging

SECRETS_UNIT = [
    "ca-cert",
    "csr",
    "certificate",
    "truststore-password",
    "keystore-password",
    "private-key",
]


class Context(WithLogging):
    """Properties and relations of the charm."""

    def __init__(self, charm: CharmBase, config: CharmConfig):
        self.model = charm.model
        self.config = config
        self.metastore_db_requirer = DatabaseRequirerData(
            self.model, POSTGRESQL_METASTORE_DB_REL, database_name=METASTORE_DATABASE_NAME
        )
        self.auth_db_requirer = DatabaseRequirerData(
            self.model,
            POSTGRESQL_AUTH_DB_REL,
            database_name=AUTHENTICATION_DATABASE_NAME,
            extra_user_roles="superuser",
        )
        self.ldap_requirer = LdapRequirer(charm=charm, relation_name=LDAP_RELATION_NAME)
        self.zookeeper_requirer_data = DatabaseRequirerData(
            self.model,
            ZOOKEEPER_REL,
            database_name=HA_ZNODE_NAME,
        )
        self.peer_app_interface = DataPeerData(
            self.model, relation_name=PEER_REL, additional_secret_fields=SECRETS_APP
        )
        self.peer_unit_interface = DataPeerUnitData(
            self.model, relation_name=PEER_REL, additional_secret_fields=SECRETS_UNIT
        )

        namespace = self.config.namespace if self.config.namespace else self.model.name
        service_account = self.config.service_account
        self.spark_service_account_interface = SparkServiceAccountRequirerData(
            self.model,
            relation_name=SPARK_SERVICE_ACCOUNT_REL,
            service_account=f"{namespace}:{service_account}",
        )

    @property
    def _spark_account_relation(self) -> Relation | None:
        """The integration hub relation."""
        return self.model.get_relation(SPARK_SERVICE_ACCOUNT_REL)

    @property
    def _zookeeper_relation(self) -> Relation | None:
        """The zookeeper relation."""
        return self.model.get_relation(ZOOKEEPER_REL)

    @property
    def _peer_relation(self) -> Relation | None:
        """The cluster peer relation."""
        return self.model.get_relation(PEER_REL)

    @property
    def _tls_relation(self) -> Relation | None:
        """The cluster peer relation."""
        return self.model.get_relation(TLS_REL)

    @property
    def _certificate_transfer_relation(self) -> Relation | None:
        """The certificate transfer relation."""
        return self.model.get_relation(CERTIFICATES_TRANSFER_RELATION_NAME)

    @property
    def _ldap_relation(self) -> Relation | None:
        """The LDAP relation."""
        return self.model.get_relation(LDAP_RELATION_NAME)

    # --- DOMAIN OBJECTS ---

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
        """The state of JDBC authentication connection."""
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
    def ldap(self) -> LDAPInfo | None:
        """The state of the LDAP relation."""
        if not self._ldap_relation:
            return None
        data = self.ldap_requirer.consume_ldap_relation_data(relation=self._ldap_relation)
        if not data:
            return None
        if any(
            param is None
            for param in [
                data.base_dn,
                data.bind_dn,
                data.bind_password,
            ]
        ) or any(
            param is None
            for param in [
                data.urls,
                data.ldaps_urls,
            ]
        ):
            self.logger.warning("LDAP relation data is incomplete or missing required fields.")
            return None
        return LDAPInfo(
            auth_method=data.auth_method,
            ldap_urls=data.urls,
            ldaps_urls=data.ldaps_urls,
            base_dn=data.base_dn,
            bind_dn=data.bind_dn,
            bind_password=data.bind_password,
            start_tls=data.starttls,
        )

    @property
    def service_account(self) -> SparkServiceAccountInfo | None:
        """The state of service account information."""
        if not self._spark_account_relation:
            return None
        return SparkServiceAccountInfo(
            self._spark_account_relation, self.spark_service_account_interface, self.model.app
        )

    @property
    def zookeeper(self) -> ZookeeperInfo | None:
        """The state of the Zookeeper information."""
        return (
            ZookeeperInfo(rel, self.zookeeper_requirer_data, rel.app)
            if (rel := self._zookeeper_relation)
            else None
        )

    def is_authentication_enabled(self) -> bool:
        """Returns whether the authentication has been enabled in the Kyuubi charm."""
        return self.auth_db is not None or self.ldap is not None

    @property
    def frontend_tls(self) -> TLSInfo | None:
        """The state of the tls configuration info."""
        if self._tls_relation:
            return TLSInfo(
                self.unit_server.keystore_password, self.unit_server.truststore_password
            )
        return None

    @property
    def backend_tls(self) -> TLSInfo | None:
        """The state of the tls configuration info."""
        if self._certificate_transfer_relation:
            return TLSInfo(
                self.unit_server.keystore_password, self.unit_server.truststore_password
            )
        return None

    # CORE COMPONENTS

    @property
    def bind_address(self) -> IPv4Address | IPv6Address | str:
        """The network binding address from the peer relation."""
        bind_address = None
        if self._peer_relation:
            if binding := self.model.get_binding(self._peer_relation):
                bind_address = binding.network.bind_address
        return bind_address or ""

    @property
    def unit_server(self) -> KyuubiServer:
        """The server state of the current running Unit."""
        return KyuubiServer(
            relation=self._peer_relation,
            data_interface=self.peer_unit_interface,
            component=self.model.unit,
        )

    @property
    def cluster(self) -> KyuubiCluster:
        """The cluster state of the current running App."""
        return KyuubiCluster(
            relation=self._peer_relation,
            data_interface=self.peer_app_interface,
            component=self.model.app,
        )

    @property
    def client_relations(self) -> set[Relation]:
        """The relations of all client applications."""
        return set(self.model.relations[KYUUBI_CLIENT_RELATION_NAME])

    @property
    def app_units(self) -> set[Unit]:
        """The peer-related units in the application."""
        if not self._peer_relation:
            return set()

        return {self.model.unit, *self._peer_relation.units}
