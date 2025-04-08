#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Hive metastore schema manager."""

from core.workload import KyuubiWorkloadBase
from utils.logging import WithLogging


class HiveMetastoreManager(WithLogging):
    """Manager encapsulating utility methods to apply Hive metastore schema."""

    def __init__(self, workload: KyuubiWorkloadBase):
        self.workload = workload

    def initialize_schema(self, schema_version: str) -> None:
        """Initialize Hive Schema."""
        self.workload.exec(
            f"/opt/hive/bin/schematool.sh -dbType postgres -initSchemaTo {schema_version}"
        )
        self.workload.restart()
