#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Definition of various model classes."""

import json
import logging
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from typing import List, MutableMapping

from charms.data_platform_libs.v0.data_interfaces import Data, DataPeerData
from ops import Application, Relation, Unit
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus
from typing_extensions import override

from common.relation.domain import RelationState
from constants import SECRETS_APP
from managers.service import ServiceManager

logger = logging.getLogger(__name__)


@dataclass
class User:
    """Class representing the user running the Pebble workload services."""

    name: str = "_daemon_"
    group: str = "_daemon_"


class Status(Enum):
    """Class bundling all statuses that the charm may fall into."""

    WAITING_PEBBLE = MaintenanceStatus("Waiting for Pebble")
    MISSING_OBJECT_STORAGE_BACKEND = BlockedStatus("Missing Object Storage backend")
    INVALID_CREDENTIALS = BlockedStatus("Invalid S3 credentials")
    MISSING_INTEGRATION_HUB = BlockedStatus("Missing integration hub relation")
    INVALID_NAMESPACE = BlockedStatus("Invalid config option: namespace")
    INVALID_SERVICE_ACCOUNT = BlockedStatus("Invalid config option: service-account")
    INSUFFICIENT_CLUSTER_PERMISSIONS = BlockedStatus(
        "Insufficient cluster permissions. Try: juju trust --scope=cluster <app-name>"
    )
    WAITING_ZOOKEEPER = MaintenanceStatus("Waiting for zookeeper credentials")
    MISSING_ZOOKEEPER = BlockedStatus(
        "Missing Zookeeper integration (which is required when there are more than one units of Kyuubi)"
    )
    WAITING_FOR_SERVICE = MaintenanceStatus("Waiting for Kyuubi service to be available...")

    ACTIVE = ActiveStatus("")


# The StateBase class should be deprecated in favor of a RelationBase class
# when secrets are enabled on S3 relation, and S3 classes have a similar
# structure with respect to the other data-platform interfaces.
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
        return self.relation_data.get("endpoint", "")

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
        return self.relation_data.get("path", "")

    @property
    def bucket(self) -> str:
        """Return the name of the S3 bucket."""
        return self.relation_data.get("bucket", "")

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


class SparkServiceAccountInfo(RelationState):
    """Requirer-side of the Integration Hub relation."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: Data,
        component: Application,
    ):
        super().__init__(relation, data_interface, component)
        self.data_interface = data_interface
        self.app = component

    def __bool__(self):
        """Return flag of whether the class is ready to be used."""
        return super().__bool__() and "service-account" in self.relation_data.keys()

    @property
    def service_account(self):
        """Service account used for Spark."""
        return self.relation_data["service-account"]

    @property
    def namespace(self):
        """Namespace used for running Spark jobs."""
        return self.relation_data["namespace"]


class ZookeeperInfo(RelationState):
    """State collection metadata for a the Zookeeper relation."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: Data,
        local_app: Application | None = None,
    ):
        super().__init__(relation, data_interface, None)
        self._local_app = local_app

    @property
    def username(self) -> str:
        """Username to connect to ZooKeeper."""
        if not self.relation:
            return ""

        return (
            self.data_interface.fetch_relation_field(
                relation_id=self.relation.id, field="username"
            )
            or ""
        )

    @property
    def password(self) -> str:
        """Password of the ZooKeeper user."""
        if not self.relation:
            return ""

        return (
            self.data_interface.fetch_relation_field(
                relation_id=self.relation.id, field="password"
            )
            or ""
        )

    @property
    def endpoints(self) -> str:
        """IP/host where ZooKeeper is located."""
        if not self.relation:
            return ""

        return (
            self.data_interface.fetch_relation_field(
                relation_id=self.relation.id, field="endpoints"
            )
            or ""
        )

    @property
    def database(self) -> str:
        """Path allocated for Kyuubi on ZooKeeper."""
        if not self.relation:
            return ""

        return (
            self.data_interface.fetch_relation_field(
                relation_id=self.relation.id, field="database"
            )
            or ""
        )

    @property
    def uris(self) -> str:
        """Comma separated connection string, containing endpoints."""
        if not self.relation:
            return ""

        return ",".join(
            sorted(  # sorting as they may be disordered
                (
                    self.data_interface.fetch_relation_field(
                        relation_id=self.relation.id, field="uris"
                    )
                    or ""
                ).split(",")
            )
        ).replace(self.database, "")

    @property
    def zookeeper_connected(self) -> bool:
        """Checks if there is an active ZooKeeper relation with all necessary data.

        Returns:
            True if ZooKeeper is currently related with sufficient relation data
                for a broker to connect with. Otherwise False
        """
        if not all([self.username, self.password, self.database, self.uris]):
            return False

        return True

    def __bool__(self) -> bool:
        """Return whether this class object has sufficient information."""
        return self.zookeeper_connected


@dataclass
class TLSInfo:
    """Class representing a information related to tls."""

    keystore_password: str
    trustore_password: str


@dataclass
class SANs:
    """Subject Alternative Name (SAN)s used to create multi-domains certificates."""

    sans_ip: list[str]
    sans_dns: list[str]


class KyuubiServer(RelationState):
    """State collection metadata for a charm unit."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: Data,
        component: Unit,
    ):
        super().__init__(relation, data_interface, component)
        self.unit = component
        self.k8s = ServiceManager(
            self.unit._backend.model_name, self.unit.name, self.unit.app.name
        )

    @property
    def unit_id(self) -> int:
        """The id of the unit from the unit name.

        e.g kyuubi/2 --> 2
        """
        return int(self.unit.name.split("/")[1])

    @property
    def hostname(self) -> str:
        """The hostname for the unit."""
        return self.relation_data.get("hostname", "")

    @property
    def fqdn(self) -> str:
        """The Fully Qualified Domain Name for the unit."""
        return self.relation_data.get("fqdn", "")

    @property
    def ip(self) -> str:
        """The IP for the unit."""
        return self.relation_data.get("ip", "")

    @property
    def host(self) -> str:
        """The hostname for the unit."""
        return f"{self.unit.name.split('/')[0]}-{self.unit_id}.{self.unit.name.split('/')[0]}-endpoints"

    # -- TLS --

    @property
    def private_key(self) -> str:
        """The private-key contents for the unit to use for TLS."""
        return self.relation_data.get("private-key", "")

    @property
    def keystore_password(self) -> str:
        """The Java Keystore password for the unit to use for TLS."""
        return self.relation_data.get("keystore-password", "")

    @property
    def truststore_password(self) -> str:
        """The Java Truststore password for the unit to use for TLS."""
        return self.relation_data.get("truststore-password", "")

    @property
    def csr(self) -> str:
        """The current certificate signing request contents for the unit."""
        return self.relation_data.get("csr", "")

    @property
    def certificate(self) -> str:
        """The certificate contents for the unit to use for TLS."""
        return self.relation_data.get("certificate", "")

    @property
    def ca(self) -> str:
        """The root CA contents for the unit to use for TLS."""
        # Backwards compatibility
        # TODO (zkclient): Remove this property and replace by "" in self.ca_cert
        ca = self.relation_data.get("ca", "")
        if ca:
            logger.warning(
                "Using 'ca' in the databag is deprecated, use 'ca_cert' instead",
                DeprecationWarning,
            )
        return ca

    @property
    def ca_cert(self) -> str:
        """The root CA contents for the unit to use for TLS."""
        return self.relation_data.get("ca-cert", self.ca)

    @property
    def internal_address(self) -> str:
        """The hostname for the unit, for internal communication."""
        return f"{self.unit.name.split('/')[0]}-{self.unit_id}.{self.unit.name.split('/')[0]}-endpoints"

    @property
    def pod_name(self) -> str:
        """The name of the K8s Pod for the unit.

        K8s-only.
        """
        return self.unit.name.replace("/", "-")

    @cached_property
    def node_ip(self) -> str:
        """The IPV4/IPV6 IP address of the Node the unit is on.

        K8s-only.
        """
        return self.k8s.get_node_ip(self.pod_name)

    @cached_property
    def loadbalancer_ip(self) -> str:
        """The IPV4/IPV6 IP address of the LoadBalancer exposing the unit.

        K8s-only.
        """
        # TODO fix when external access is merged.
        return ""


class KyuubiCluster(RelationState):
    """State collection metadata for the charm application."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: DataPeerData,
        component: Application,
    ):
        super().__init__(relation, data_interface, component)
        self.data_interface = data_interface
        self.app = component

    @override
    def update(self, items: dict[str, str]) -> None:
        """Overridden update to allow for same interface, but writing to local app bag."""
        if not self.relation:
            return

        for key, value in items.items():
            if key in SECRETS_APP or key.startswith("relation-"):
                if value:
                    self.data_interface.set_secret(self.relation.id, key, value)
                else:
                    self.data_interface.delete_secret(self.relation.id, key)
            else:
                self.data_interface.update_relation_data(self.relation.id, {key: value})

    # -- TLS --

    @property
    def tls(self) -> bool:
        """Flag to check if TLS is enabled for the cluster."""
        return self.relation_data.get("tls", "") == "enabled"
