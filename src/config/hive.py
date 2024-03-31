#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Hive configurations."""

from typing import Optional

from constants import (
    METASTORE_DATABASE_NAME,
)
from database import DatabaseConnectionInfo
from utils import WithLogging


class HiveConfig(WithLogging):
    """Spark History Server Configuration."""

    def __init__(self, db_info: Optional[DatabaseConnectionInfo]):
        self.db_info = db_info

    @property
    def _db_conf(self) -> dict[str, str]:
        """Return a dictionary representation of hive configuration."""
        if not self.db_info:
            return {}
        return {
            "javax.jdo.option.ConnectionURL": self._get_db_connection_url(),
            "javax.jdo.option.ConnectionDriverName": "org.postgresql.Driver",
            "javax.jdo.option.ConnectionUserName": self.db_info.username,
            "javax.jdo.option.ConnectionPassword": self.db_info.password,
            "datanucleus.autoCreateSchema": "true",
            "datanucleus.fixedDatastore": "true",
            "datanucleus.autoCreateTables": "true",
            "hive.server2.enable.doAs": "false",
            "hive.metastore.schema.verification": "false",
        }

    def _get_db_connection_url(self) -> str:
        endpoint = self.db_info.endpoint
        return (
            f"jdbc:postgresql://{endpoint}/{METASTORE_DATABASE_NAME}?createDatabaseIfNotExist=true"
        )

    def to_dict(self) -> dict[str, str]:
        """Return the dict representation of the configuration file."""
        return self._db_conf

    @property
    def contents(self) -> str:
        """Return configuration contents formatted to be consumed by pebble layer."""
        properties = ""
        for name, value in self.to_dict().items():
            properties += (
                "<property>\n"
                f"    <name>{name}</name>\n"
                f"    <value>{value}</value>\n"
                "</property>\n"
            )
        return '<?xml version="1.0"?>\n' "<configuration>\n" f"{properties}" "</configuration>"
