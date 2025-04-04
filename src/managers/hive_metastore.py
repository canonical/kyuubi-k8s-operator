#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Hive metastore schema manager."""

from utils.logging import WithLogging


class HiveMetastoreManager(WithLogging):
    """Manager encapsulating utility methods to apply Hive metastore schema."""

    HIVE_SCHEMA_SCRIPTS = [
        "hive-schema-1.2.0.postgres.sql",
        "upgrade-1.2.0-to-2.0.0.postgres.sql",
        "upgrade-2.0.0-to-2.1.0.postgres.sql"
    ]
