#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 connection utility classes and methods."""

from dataclasses import dataclass

import psycopg2

from constants import POSTGRESQL_DEFAULT_DATABASE
from utils import WithLogging


@dataclass
class DatabaseConnectionInfo(WithLogging):
    """Class representing credentials and endpoints to connect to Postgres database."""

    endpoint: str
    username: str
    password: str

    def verify(self) -> bool:
        """Verify the PostgreSQL connection."""
        try:
            connection = psycopg2.connect(
                host=self.endpoint,
                user=self.username,
                password=self.password,
                dbname=POSTGRESQL_DEFAULT_DATABASE,
            )
            connection.close()
            return True
        except Exception as e:
            self.logger.warning(f"PostgreSQL connection not successful. Reason: {e}")
            return False
