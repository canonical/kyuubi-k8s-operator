#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Implementation and blue-print for Kyuubi workloads."""

from abc import abstractmethod
from pathlib import Path

from core.domain import User
from workload import AbstractWorkload


class KyuubiPaths:
    """Object to store common paths for Kyuubi."""

    def __init__(self, conf_path: Path | str, keytool: str):
        self.conf_path = conf_path if isinstance(conf_path, Path) else Path(conf_path)
        self.keytool = keytool

    @property
    def spark_properties(self) -> Path:
        """Return the path of the spark-properties file."""
        return self.conf_path / "spark-properties.conf"


class KyuubiWorkloadBase(AbstractWorkload):
    """Base interface for common workload operations."""

    paths: KyuubiPaths
    user: User

    def restart(self) -> None:
        """Restarts the workload service."""
        self.stop()
        self.start()

    @abstractmethod
    def active(self) -> bool:
        """Checks that the workload is active."""
        ...

    @abstractmethod
    def ready(self) -> bool:
        """Checks that the container/snap is ready."""
        ...
