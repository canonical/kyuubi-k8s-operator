#!/usr/bin/env python3

# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

"""Hive metastore schema manager."""

import logging

import ops

from core.workload import KyuubiWorkloadBase
from utils.logging import WithLogging

logger = logging.getLogger(__name__)


class HiveMetastoreManager(WithLogging):
    """Manager encapsulating utility methods to apply Hive metastore schema."""

    METASTORE_DB_TYPE = "postgres"

    def __init__(self, workload: KyuubiWorkloadBase):
        self.workload = workload

    def _run_schematool_command(
        self,
        *args,
        dry_run: bool = False,
        username: str | None = None,
        password: str | None = None,
    ) -> tuple[int, str, str]:
        command_args = [self.workload.paths.schematool_bin, "-dbType", self.METASTORE_DB_TYPE]
        if dry_run:
            command_args.append("-dryRun")
        if len(args) > 0:
            command_args.extend(args)

        if username is not None:
            command_args.extend(["-userName", username])
        if password is not None:
            command_args.extend(["-passWord", password])

        try:
            out = self.workload.exec(" ".join(command_args))
            return 0, out, ""
        except ops.pebble.ExecError as e:
            # ExceError is raised when the return code is not 0
            return 1, e.stdout or "", e.stderr or ""
        except Exception as e:
            logger.exception(e)
            return 1, "", str(e)

    def is_metastore_valid(self, username: str | None = None, password: str | None = None) -> bool:
        """Validate the metastore schema and return if it is valid."""
        retcode, _, _ = self._run_schematool_command(
            "-validate", username=username, password=password
        )
        return retcode == 0

    def initialize(
        self, schema_version: str, username: str | None = None, password: str | None = None
    ) -> None:
        """Initialize the Hive schema to the given schema version.

        If the schema is already valid, skip initialization. If the schema is not valid, attempt to initialize it.

        If the username and password are provided, they will be used for connection with Postgres. Else, the values
        in hive-site.xml is picked up by the schematool command.
        """
        # First check that if the metastore schema is already valid
        if self.is_metastore_valid(username=username, password=password):
            logger.info(
                "Metastore schema is already initialized and valid. Skipping initialization"
            )
            return

        # Attempt initialization with dry run first
        retcode, stdout, stderr = self._run_schematool_command(
            "-initSchemaTo", schema_version, dry_run=True, username=username, password=password
        )
        if retcode != 0:
            logger.error(
                f"Cannot safely initialize Hive schema in metastore database. stdout={stdout}; stderr={stderr}"
            )
            return

        # Attempt actual initialization
        retcode, stdout, stderr = self._run_schematool_command(
            "-initSchemaTo", schema_version, username=username, password=password
        )
        if retcode != 0:
            logger.error(
                f"Hive schema initialization failed for metastore database. stdout={stdout}; stderr={stderr}"
            )
            return

        logger.info(f"Metastore database initialized with Hive schema {schema_version}")
