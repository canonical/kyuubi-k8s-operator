#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Kyuubi workload configurations."""

from typing import Optional

from constants import AUTHENTICATION_TABLE_NAME
from database import DatabaseConnectionInfo
from utils.logging import WithLogging


class KyuubiConfig(WithLogging):
    """Kyuubi Configurations."""

    def __init__(self, db_info: Optional[DatabaseConnectionInfo]):
        self.db_info = db_info

    def _get_db_connection_url(self) -> str:
        endpoint = self.db_info.endpoint
        return f"jdbc:postgresql://{endpoint}/{self.db_info.dbname}"

    def _get_authentication_query(self) -> str:
        return (
            f"SELECT 1 FROM {AUTHENTICATION_TABLE_NAME} "
            "WHERE username=${user} AND passwd=${password}"
        )

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

    def to_dict(self) -> dict[str, str]:
        """Return the dict representation of the configuration file."""
        return self._auth_conf

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
