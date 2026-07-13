#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Kyuubi workload configurations."""

from constants import AUTHENTICATION_TABLE_NAME
from core.config import CharmConfig
from core.domain import DatabaseConnectionInfo, LDAPInfo, TLSInfo, ZookeeperInfo
from utils.logging import WithLogging


class KyuubiConfig(WithLogging):
    """Kyuubi Configurations."""

    def __init__(
        self,
        charm_config: CharmConfig,
        db_info: DatabaseConnectionInfo | None,
        zookeeper_info: ZookeeperInfo | None,
        frontend_tls_info: TLSInfo | None,
        ldap_info: LDAPInfo | None,
        keystore_path: str,
    ):
        self.charm_config = charm_config
        self.db_info = db_info
        self.zookeeper_info = zookeeper_info
        self.frontend_tls = frontend_tls_info
        self.ldap = ldap_info
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
            "kyuubi.frontend.protocols": "THRIFT_BINARY,REST",
            "kyuubi.frontend.rest.bind.host": "127.0.0.1",
        }

        if self.charm_config.gpu_enable:
            conf.update(
                {
                    "kyuubi.session.engine.idle.timeout": "PT3M",
                }
            )

        if self.charm_config.profile == "testing":
            conf.update(
                {
                    "kyuubi.session.engine.idle.timeout": "PT1M",
                }
            )
        return conf

    @property
    def _auth_conf(self) -> dict[str, str]:
        """Return authentication configurations."""
        if self.db_info and self.ldap:
            self.logger.warning(
                "Both JDBC and LDAP authentication are configured. "
                "Kyuubi configurations are not generated, the charm will go to blocked state."
            )
            return {}
        elif not (self.db_info or self.ldap):
            self.logger.warning(
                "Neither JDBC nor LDAP authentication is configured. "
                "Kyuubi configurations are not generated, the charm will go to blocked state."
            )
            return {}
        if self.db_info:
            return self._jdbc_auth_conf
        elif self.ldap:
            return self._ldap_auth_conf
        else:
            return {}

    @property
    def _jdbc_auth_conf(self) -> dict[str, str]:
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
    def _ldap_auth_conf(self) -> dict[str, str]:
        if not self.ldap:
            return {}
        urls = []
        if self.ldap.ldaps_urls:
            urls.extend(self.ldap.ldaps_urls)
        # Disabling LDAP + StartTLS, and always using LDAPs
        # if self.ldap.ldap_urls:
        #     urls.extend(self.ldap.ldap_urls)
        ldap_url_string = " ".join(urls)
        return {
            "kyuubi.authentication": "LDAP",
            "kyuubi.authentication.ldap.baseDN": self.ldap.base_dn,
            # "kyuubi.authentication.ldap.domain": "glauth.com",
            "kyuubi.authentication.ldap.binddn": self.ldap.bind_dn,
            "kyuubi.authentication.ldap.bindpw": self.ldap.bind_password,
            "kyuubi.authentication.ldap.url": ldap_url_string,
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
        if not self.frontend_tls or not self.keystore_path:
            return {}
        return {
            "kyuubi.frontend.ssl.keystore.password": self.frontend_tls.keystore_password,
            "kyuubi.frontend.ssl.keystore.path": self.keystore_path,
            "kyuubi.frontend.ssl.keystore.type": "PKCS12",
            "kyuubi.frontend.thrift.binary.ssl.enabled": "true",
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
