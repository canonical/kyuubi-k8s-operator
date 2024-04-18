#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 connection utility classes and methods."""

from dataclasses import dataclass

import psycopg2

from constants import (
    POSTGRESQL_DEFAULT_DATABASE,
)
from utils.logging import WithLogging


@dataclass
class DatabaseConnectionInfo(WithLogging):
    """Class representing credentials and endpoints to connect to Postgres database."""

    endpoint: str
    username: str
    password: str
    dbname: str

    def execute(self, query: str, vars=None, dbname: str = None) -> tuple[bool, list]:
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
            dbname = self.dbname
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(
                host=self.endpoint,
                user=self.username,
                password=self.password,
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
            self.logger.warning(f"PostgreSQL connection not successful. Reason: {e}")
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
