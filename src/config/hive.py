#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Hive related configurations."""

from typing import Optional
from xml.etree import ElementTree

from constants import (
    METASTORE_DATABASE_NAME,
)
from database import DatabaseConnectionInfo
from utils.logging import WithLogging


class HiveConfig(WithLogging):
    """Hive Configuration."""

    def __init__(self, db_info: Optional[DatabaseConnectionInfo]):
        self.db_info = db_info

    def _get_db_connection_url(self) -> str:
        endpoint = self.db_info.endpoint
        return (
            f"jdbc:postgresql://{endpoint}/{METASTORE_DATABASE_NAME}?createDatabaseIfNotExist=true"
        )

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

    def to_dict(self) -> dict[str, str]:
        """Return the dict representation of the configuration file."""
        return self._db_conf

    @property
    def contents(self) -> str:
        """Return configuration contents formatted to be saved in hive-site.xml."""
        header = '<?xml version="1.0"?>'
        root = ElementTree.Element("configuration")
        for name, value in self.to_dict().items():
            prop = ElementTree.SubElement(root, "property")
            name_element = ElementTree.SubElement(prop, "name")
            name_element.text = name
            value_element = ElementTree.SubElement(prop, "value")
            value_element.text = value
        body = ElementTree.tostring(root, encoding="unicode")

        return f"{header}\n{body}\n"
