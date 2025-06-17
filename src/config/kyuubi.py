#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Kyuubi workload configurations."""

from constants import AUTHENTICATION_TABLE_NAME
from core.domain import DatabaseConnectionInfo, TLSInfo, ZookeeperInfo
from utils.logging import WithLogging


class KyuubiConfig(WithLogging):
    """Kyuubi Configurations."""

    def __init__(
        self,
        db_info: DatabaseConnectionInfo | None,
        zookeeper_info: ZookeeperInfo | None,
        tls_info: TLSInfo | None,
        keystore_path: str,
    ):
        self.db_info = db_info
        self.zookeeper_info = zookeeper_info
        self.tls = tls_info
        self.keystore_path = keystore_path

    def _get_db_connection_url(self) -> str:
        match self.db_info:
            case None:
                return ""
            case db:
                return f"jdbc:postgresql://{db.endpoint}/{db.dbname}"

    def _get_authentication_query(self) -> str:
        return (
            f"SELECT 1 FROM {AUTHENTICATION_TABLE_NAME} "
            "WHERE username=${user} AND passwd=crypt(${password}, passwd);"
        )

    def _get_zookeeper_auth_digest(self) -> str:
        """Return auth digest string to connect to ZooKeeper."""
        if not self.zookeeper_info:
            return ""
        username = self.zookeeper_info.username
        password = self.zookeeper_info.password
        return f"{username}:{password}"

    @property
    def _base_conf(self) -> dict[str, str]:
        """Return base Kyuubi configurations."""
        conf = {
            "kyuubi.session.engine.initialize.timeout": "PT10M",
        }
        return conf

    @property
    def _auth_conf(self) -> dict[str, str]:
        if not self.db_info:
            return {}
        return {
            "kyuubi.authentication": "JDBC",
            "kyuubi.authentication.jdbc.driver.class": "org.postgresql.Driver",
            "kyuubi.authentication.jdbc.url": self._get_db_connection_url(),
            "kyuubi.authentication.jdbc.user": self.db_info.username,
            "kyuubi.authentication.jdbc.password": self.db_info.password,
            "kyuubi.authentication.jdbc.query": self._get_authentication_query(),
        }

    @property
    def _ha_conf(self) -> dict[str, str]:
        if not self.zookeeper_info:
            return {}
        return {
            "kyuubi.ha.addresses": self.zookeeper_info.uris,
            "kyuubi.ha.namespace": self.zookeeper_info.database,
            "kyuubi.ha.zookeeper.auth.type": "DIGEST",
            "kyuubi.ha.zookeeper.auth.digest": self._get_zookeeper_auth_digest(),
        }

    @property
    def _tls_conf(self) -> dict[str, str]:
        if not self.tls:
            return {}
        return {
            "kyuubi.frontend.ssl.keystore.password": self.tls.keystore_password,
            "kyuubi.frontend.ssl.keystore.path": self.keystore_path,
            "kyuubi.frontend.ssl.keystore.type": "PKCS12",
            "kyuubi.frontend.thrift.binary.ssl.enabled": "true",
            # enable thrift http frontend with certificate
            "kyuubi.frontend.thrift.http.ssl.keystore.password": self.tls.keystore_password,
            "kyuubi.frontend.thrift.http.ssl.keystore.path": self.keystore_path,
            "kyuubi.frontend.thrift.http.use.SSL": "true",
        }

    def to_dict(self) -> dict[str, str]:
        """Return the dict representation of the configuration file."""
        return self._base_conf | self._auth_conf | self._ha_conf | self._tls_conf

    @property
    def contents(self) -> str:
        """Return configuration contents formatted to be consumed by pebble layer."""
        dict_content = self.to_dict()

        return "\n".join(
            [
                f"{key}={value}"
                for key in sorted(dict_content.keys())
                if (value := dict_content[key])
            ]
        )
