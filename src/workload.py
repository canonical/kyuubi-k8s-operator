"""This model contains classes and methods related to Kyuubi workload."""

import socket

from models import User
from ops.model import Container
from utils import ContainerFile, IOMode, WithLogging


class KyuubiServer(WithLogging):
    """The abstraction of Kyuubi workload container."""

    KYUUBI_WORKDIR = "/opt/kyuubi"
    KYUUBI_SERVICE = "kyuubi"
    CONTAINER_LAYER = "kyuubi"
    SPARK_PROPERTIES = "/etc/spark8t/conf/spark-defaults.conf"
    JDBC_PORT = 10009

    def __init__(self, container: Container, user: User = User()):
        self.container = container
        self.user = User

    def get_spark_configuration_file(self, mode: IOMode) -> ContainerFile:
        """Return the configuration file for Spark History server."""
        daemon_user = User(name="_daemon_", group="_daemon_")
        return ContainerFile(self.container, daemon_user, self.SPARK_PROPERTIES, mode)

    def get_jdbc_endpoint(self) -> str:
        """Return the JDBC endpoint to connect to Kyuubi server."""
        hostname = socket.getfqdn()
        ip_address = socket.gethostbyname(hostname)
        return f"jdbc:hive2://{ip_address}:{self.JDBC_PORT}/"

    @property
    def _kyuubi_server_layer(self):
        """Return a dictionary representing a Pebble layer."""
        return {
            "summary": "kyuubi layer",
            "description": "pebble config layer for kyuubi",
            "services": {
                self.KYUUBI_SERVICE: {
                    "override": "merge",
                    "startup": "enabled",
                }
            },
        }

    def start(self):
        """Execute business-logic for starting the workload."""
        services = self.container.get_plan().services

        spark_configuration_file = self.get_spark_configuration_file(IOMode.READ)
        if not spark_configuration_file.exists():
            self.logger.error(f"{spark_configuration_file.path} not found")
            raise FileNotFoundError(spark_configuration_file.path)

        if services[self.KYUUBI_SERVICE].startup != "enabled":
            self.logger.info("Adding kyuubi pebble layer...")
            self.container.add_layer(self.CONTAINER_LAYER, self._kyuubi_server_layer)

        self.container.restart(self.KYUUBI_SERVICE)

    def stop(self):
        """Execute business-logic for stopping the workload."""
        self.logger.info("Stopping kyuubi pebble service...")
        self.container.stop(self.KYUUBI_SERVICE)

    def ready(self) -> bool:
        """Check whether the service is ready to be used."""
        return self.container.can_connect()

    def health(self) -> bool:
        """Return the health of the service."""
        return self.container.get_service(self.KYUUBI_SERVICE).is_running()
