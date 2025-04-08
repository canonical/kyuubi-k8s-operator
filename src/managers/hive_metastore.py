#!/usr/bin/env python3

# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

"""Hive metastore schema manager."""

import logging
import subprocess

import ops

from core.workload import KyuubiWorkloadBase
from utils.logging import WithLogging

logger = logging.getLogger(__name__)


class HiveMetastoreManager(WithLogging):
    """Manager encapsulating utility methods to apply Hive metastore schema."""

    METASTORE_DB_TYPE = "postgres"

    def __init__(self, workload: KyuubiWorkloadBase):
        self.workload = workload

    def initialize_schema(self, schema_version: str) -> None:
        """Initialize Hive Schema."""
        command = " ".join(
            [
                self.workload.paths.schematool_bin,
                "-dbType",
                self.METASTORE_DB_TYPE,
                "-initSchemaTo",
                schema_version,
            ]
        )
        try:
            self.workload.exec(command)
            self.workload.restart()
        except (subprocess.CalledProcessError, ops.pebble.ExecError) as e:
            logger.error(str(e.stderr))
            raise e
