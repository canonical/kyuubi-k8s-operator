#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Definition of various model classes."""

import json
from dataclasses import dataclass
from enum import Enum
from typing import List, MutableMapping

from ops import Application, CharmBase, Relation, Unit
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus

from constants import (
    NAMESPACE_CONFIG_NAME,
    SERVICE_ACCOUNT_CONFIG_NAME,
)


@dataclass
class User:
    """Class representing the user running the Pebble workload services."""

    name: str = "_daemon_"
    group: str = "_daemon_"


class Status(Enum):
    """Class bundling all statuses that the charm may fall into."""

    WAITING_PEBBLE = MaintenanceStatus("Waiting for Pebble")
    MISSING_S3_RELATION = BlockedStatus("Missing S3 relation")
    INVALID_CREDENTIALS = BlockedStatus("Invalid S3 credentials")
    INVALID_NAMESPACE = BlockedStatus("Invalid config option: namespace")
    INVALID_SERVICE_ACCOUNT = BlockedStatus("Invalid config option: service-account")

    ACTIVE = ActiveStatus("")


class StateBase:
    """Base state object."""

    def __init__(self, relation: Relation | None, component: Unit | Application):
        self.relation = relation
        self.component = component

    @property
    def relation_data(self) -> MutableMapping[str, str]:
        """The raw relation data."""
        if not self.relation:
            return {}

        return self.relation.data[self.component]

    def update(self, items: dict[str, str]) -> None:
        """Writes to relation_data."""
        if not self.relation:
            return

        self.relation_data.update(items)

    def clean(self) -> None:
        """Clean the content of the relation data."""
        if not self.relation:
            return
        self.relation.data[self.component] = {}


class S3ConnectionInfo(StateBase):
    """Class representing credentials and endpoints to connect to S3."""

    def __init__(self, relation: Relation, component: Application):
        super().__init__(relation, component)

    @property
    def endpoint(self) -> str | None:
        """Return endpoint of the S3 bucket."""
        return self.relation_data.get("endpoint", None)

    @property
    def access_key(self) -> str:
        """Return the access key."""
        return self.relation_data.get("access-key", "")

    @property
    def secret_key(self) -> str:
        """Return the secret key."""
        return self.relation_data.get("secret-key", "")

    @property
    def path(self) -> str:
        """Return the path in the S3 bucket."""
        return self.relation_data["path"]

    @property
    def bucket(self) -> str:
        """Return the name of the S3 bucket."""
        return self.relation_data["bucket"]

    @property
    def tls_ca_chain(self) -> List[str] | None:
        """Return the CA chain (when applicable)."""
        return (
            json.loads(ca_chain)
            if (ca_chain := self.relation_data.get("tls-ca-chain", ""))
            else None
        )

    @property
    def log_dir(self) -> str:
        """Return the full path to the object."""
        return f"s3a://{self.bucket}/{self.path}"


@dataclass
class DatabaseConnectionInfo:
    """Class representing a information related to a database connection."""

    endpoint: str
    username: str
    password: str
    dbname: str


class ServiceAccountInfo:
    """Class representing service account and namespace to be used by Kyuubi.

    For the time being, this is being read from the config options. However, when Kyuubi
    is fully integrated with the integration hub, this will be read from the integration hub
    relation in a pattern similar to S3ConnectionInfo and DatabaseConnectionInfo.
    """

    def __init__(self, charm: CharmBase):
        self.charm = charm

    @property
    def namespace(self) -> str | None:
        """Return the namespace."""
        return self.charm.config[NAMESPACE_CONFIG_NAME]

    @property
    def service_account(self) -> str | None:
        """Return the name of service account."""
        return self.charm.config[SERVICE_ACCOUNT_CONFIG_NAME]
