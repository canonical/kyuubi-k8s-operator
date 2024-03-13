from dataclasses import dataclass
from enum import Enum
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus


@dataclass
class User:
    """Class representing the user running the Pebble workload services."""

    name: str = "spark"
    group: str = "spark"



class Status(Enum):
    """Class bundling all statuses that the charm may fall into."""

    WAITING_PEBBLE = MaintenanceStatus("Waiting for Pebble")
    MISSING_S3_RELATION = BlockedStatus("Missing S3 relation")
    INVALID_CREDENTIALS = BlockedStatus("Invalid S3 credentials")
    MISSING_INGRESS_RELATION = BlockedStatus("Missing INGRESS relation")
    INVALID_NAMESPACE = BlockedStatus("Invalid config option: namespace")
    INVALID_SERVICE_ACCOUNT = BlockedStatus("Invalid config option: service-account")

    ACTIVE = ActiveStatus("")
