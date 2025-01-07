#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Implementation and blue-print for Kyuubi workloads."""

from abc import abstractmethod
from pathlib import Path

from common.workload import AbstractWorkload


class KyuubiPaths:
    """Object to store common paths for Kyuubi."""

    def __init__(
        self,
        conf_path: Path | str,
        spark_conf_path: Path | str,
        kyuubi_root: Path | str,
        keytool: str,
    ):
        self.conf_path = conf_path if isinstance(conf_path, Path) else Path(conf_path)
        self.spark_conf_path = (
            spark_conf_path if isinstance(spark_conf_path, Path) else Path(spark_conf_path)
        )
        self.kyuubi_root = kyuubi_root if isinstance(kyuubi_root, Path) else Path(kyuubi_root)
        self.keytool = keytool

    @property
    def server_key(self) -> str:
        """The private-key for the service to identify itself with for TLS auth."""
        return f"{self.conf_path}/server.key"

    @property
    def ca(self) -> str:
        """The shared cluster CA."""
        return f"{self.conf_path}/ca.pem"

    @property
    def certificate(self) -> str:
        """The certificate for the service to identify itself with for TLS auth."""
        return f"{self.conf_path}/server.pem"

    @property
    def truststore(self) -> str:
        """The Java Truststore containing trusted CAs + certificates."""
        return f"{self.conf_path}/truststore.jks"

    @property
    def keystore(self) -> str:
        """The Java Truststore containing service private-key and signed certificates."""
        return f"{self.conf_path}/keystore.p12"

    @property
    def spark_properties(self) -> str:
        """The file where spark properties are stored."""
        return f"{self.spark_conf_path}/spark-defaults.conf"

    @property
    def hive_properties(self) -> str:
        """The file where spark properties are stored."""
        return f"{self.spark_conf_path}/hive-site.xml"

    @property
    def kyuubi_properties(self) -> str:
        """The file where kyuubi properties are stored."""
        return f"{self.conf_path}/kyuubi-defaults.conf"

    @property
    def kyuubi_logs_folder(self) -> str:
        """The folder of the Kyuubi logs."""
        return f"{self.kyuubi_root}/logs"

    @property
    def kyuubi_version_file(self) -> str:
        """The Kyuubi version file."""
        return f"{self.kyuubi_root}/RELEASE"


class KyuubiWorkloadBase(AbstractWorkload):
    """Base interface for common workload operations."""

    paths: KyuubiPaths

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
