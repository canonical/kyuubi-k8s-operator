from ops.model import Container
from utils import WithLogging, IOMode, ContainerFile
from models import User

class KyuubiServer(WithLogging):
    SPARK_WORKDIR = "/opt/spark"
    KYUUBI_WORKDIR = "/opt/kyuubi"
    KYUUBI_SERVICE = "kyuubi"
    CONTAINER_LAYER = "kyuubi"
    SPARK_PROPERTIES = f"/etc/spark8t/conf/spark-defaults.conf"

    def __init__(self, container: Container, user: User = User()):
        self.container = container
        self.user = User

    def get_spark_configuration_file(self, mode: IOMode) -> ContainerFile:
        """Return the configuration file for Spark History server."""
        daemon_user = User(name="_daemon_", group="_daemon_")
        return ContainerFile(self.container, daemon_user, self.SPARK_PROPERTIES, mode)
    
    def get_jdbc_endpoint(self) -> str:
        # TODO: construct this hostname from k8s hostname?
        hostname = "kyuubi-0.kyuubi-endpoints.kyuubi.svc.cluster.local"
        port = 10009
        return f"jdbc:hive2://{hostname}:{port}/"
        # kyuubi_logs_file = ContainerFile(self.container, )
    
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
            self.container.add_layer(
                self.CONTAINER_LAYER, self._kyuubi_server_layer
            )

        self.container.restart(self.KYUUBI_SERVICE)

    def stop(self):
        """Execute business-logic for stopping the workload."""
        self.logger.info("Stopping kyuubi pebble service...")
        self.container.stop(self.HISTORY_SERVER_SERVICE)

    def ready(self) -> bool:
        """Check whether the service is ready to be used."""
        return self.container.can_connect()

    def health(self) -> bool:
        """Return the health of the service."""
        return self.container.get_service(self.HISTORY_SERVER_SERVICE).is_running()
        # We could use pebble health checks here