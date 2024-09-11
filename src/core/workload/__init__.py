#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Implementation and blue-print for Kyuubi workloads."""

from abc import abstractmethod

from common.workload import AbstractWorkload


class KyuubiWorkloadBase(AbstractWorkload):
    """Base interface for common workload operations."""

    SPARK_PROPERTIES_FILE = "/etc/spark8t/conf/spark-defaults.conf"
    HIVE_CONFIGURATION_FILE = "/etc/spark8t/conf/hive-site.xml"
    KYUUBI_CONFIGURATION_FILE = "/opt/kyuubi/conf/kyuubi-defaults.conf"
    KYUUBI_ROOT = "/opt/kyuubi"
    KYUUBI_LOGS = KYUUBI_ROOT + "/logs"
    KYUUBI_VERSION_FILE = KYUUBI_ROOT + "/RELEASE"

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
