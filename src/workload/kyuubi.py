#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Module containing all business logic related to the workload."""
import json

import ops.pebble
from ops.model import Container

from workload.k8s import K8sWorkload
from utils.logging import WithLogging
from core.domain import User
from workload.base import KyuubiWorkloadBase

import re
import socket

from ops.model import Container

from constants import (
    HIVE_CONFIGURATION_FILE,
    JDBC_PORT,
    KYUUBI_CONFIGURATION_FILE,
    KYUUBI_CONTAINER_NAME,
    KYUUBI_SERVICE_NAME,
    KYUUBI_VERSION_FILE,
    SPARK_PROPERTIES_FILE,
)
from core.domain import User
from utils.io import ContainerFile, IOMode
from utils.logging import WithLogging


class KyuubiWorkload(KyuubiWorkloadBase, K8sWorkload, WithLogging):
    """Class representing workload implementation for Kyuubi on K8s"""

    def __init__(self, container: Container, user: User = User()):
        self.container = container
        self.user = user

    def get_spark_configuration_file(self, mode: IOMode) -> ContainerFile:
        """Return the configuration file for Spark."""
        return ContainerFile(self.container, self.user, SPARK_PROPERTIES_FILE, mode)

    def get_hive_configuration_file(self, mode: IOMode) -> ContainerFile:
        """Return the configuration file for Hive parameters."""
        return ContainerFile(self.container, self.user, HIVE_CONFIGURATION_FILE, mode)

    def get_kyuubi_configuration_file(self, mode: IOMode) -> ContainerFile:
        """Return the configuration file for Hive parameters."""
        return ContainerFile(self.container, self.user, KYUUBI_CONFIGURATION_FILE, mode)

    def get_jdbc_endpoint(self) -> str:
        """Return the JDBC endpoint to connect to Kyuubi server."""
        hostname = socket.getfqdn()
        ip_address = socket.gethostbyname(hostname)
        return f"jdbc:hive2://{ip_address}:{JDBC_PORT}/"

    @property
    def _kyuubi_server_layer(self):
        """Return a dictionary representing a Pebble layer."""
        return {
            "summary": "kyuubi layer",
            "description": "pebble config layer for kyuubi",
            "services": {
                KYUUBI_SERVICE_NAME: {
                    "override": "merge",
                    "startup": "enabled",
                }
            },
        }

    def start(self):
        """Execute business-logic for starting the workload."""
        services = self.container.get_plan().services
        self.logger.info(f"Pebble services: {services}")

        spark_configuration_file = self.get_spark_configuration_file(IOMode.READ)
        if not spark_configuration_file.exists():
            self.logger.error(f"{spark_configuration_file.path} not found")
            raise FileNotFoundError(spark_configuration_file.path)

        if services[KYUUBI_SERVICE_NAME].startup != "enabled":
            self.logger.info("Adding kyuubi pebble layer...")
            self.container.add_layer(KYUUBI_CONTAINER_NAME, self._kyuubi_server_layer)

        self.container.restart(KYUUBI_SERVICE_NAME)

    def stop(self):
        """Execute business-logic for stopping the workload."""
        self.logger.info("Stopping kyuubi pebble service...")
        if self.ready():
            self.container.stop(KYUUBI_SERVICE_NAME)

    def ready(self) -> bool:
        """Check whether the service is ready to be used."""
        return self.container.can_connect()

    def active(self) -> bool:
        """Return the health of the service."""
        try:
            service = self.container.get_service(KYUUBI_SERVICE_NAME)
        except ops.pebble.ConnectionError:
            self.logger.debug(f"Service {KYUUBI_SERVICE_NAME} not running")
            return False
        return service.is_running()

    @property
    def kyuubi_version(self):
        """Return the version of Kyuubi."""
        version_pattern = r"Kyuubi (?P<version>[\d\.]+)"
        with ContainerFile(
            self.container, self.user, KYUUBI_VERSION_FILE, IOMode.READ
        ) as version_file:
            contents = version_file.read()
            version = re.search(version_pattern, contents)
            if version:
                return version.group("version")
        return ""
