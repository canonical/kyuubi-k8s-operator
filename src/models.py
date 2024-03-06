from dataclasses import dataclass


@dataclass
class User:
    """Class representing the user running the Pebble workload services."""

    name: str = "spark"
    group: str = "spark"


