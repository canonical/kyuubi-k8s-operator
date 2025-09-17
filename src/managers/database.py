#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Database connection manager."""

import psycopg2

from constants import (
    POSTGRESQL_DEFAULT_DATABASE,
)
from core.domain import DatabaseConnectionInfo
from utils.logging import WithLogging


class DatabaseManager(WithLogging):
    """Manager class encapsulating various database operations."""

    def __init__(self, db_info: DatabaseConnectionInfo) -> None:
        self.db_info = db_info

    def execute(self, query: str, vars=None, dbname: str | None = None) -> tuple[bool, list]:
        """Execute a SQL query by connecting to a given database.

        Args:
            dbname (str): The name of the database to connect to while executing the query
            query (str): The query to be executed
            vars (_type_, optional): The variables to be substituted to placeholders in `query`. Defaults to None.

        Returns:
            tuple[bool, list]: A boolean that signifies whether the query was executed successfully
                and a list of the rows that were returned by the query. The list is empty if no
                rows were returned when executing the query.
        """
        if not dbname:
            dbname = self.db_info.dbname
        connection = None
        cursor = None
        host, port = self.db_info.endpoint.split(":")
        try:
            connection = psycopg2.connect(
                host=host,
                port=int(port),
                user=self.db_info.username,
                password=self.db_info.password,
                dbname=dbname,
            )
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute(query=query, vars=vars)
            connection.commit()
            try:
                result = cursor.fetchall()
            except psycopg2.ProgrammingError:
                result = []
            return True, result
        except Exception as e:
            self.logger.warning(f"PostgreSQL query not successful. Reason: {e}")
            return False, []
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def verify(
        self,
    ) -> bool:
        """Verify whether the database connection is valid or not."""
        status, _ = self.execute(POSTGRESQL_DEFAULT_DATABASE, "SELECT 1;")
        return status
