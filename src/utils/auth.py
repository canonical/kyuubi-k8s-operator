#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 connection utility classes and methods."""

import secrets
import string
from dataclasses import dataclass

from constants import (
    AUTHENTICATION_TABLE_NAME,
    DEFAULT_ADMIN_USERNAME,
    POSTGRESQL_DEFAULT_DATABASE,
)
from database import DatabaseConnectionInfo
from utils.utils import WithLogging


@dataclass
class Authentication(WithLogging):
    """Class representing authentication."""

    database: DatabaseConnectionInfo

    def create_authentication_table(self) -> bool:
        """Create authentication table in the authentication database."""
        self.logger.info("Creating authentication table...")
        query = f"""
            CREATE TABLE {AUTHENTICATION_TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                username VARCHAR(100) UNIQUE NOT NULL,
                passwd VARCHAR(255) NOT NULL
            );
        """
        status, _ = self.database.execute(query)
        return status

    def generate_password(self) -> str:
        """Generate and return a random password string."""
        choices = string.ascii_letters + string.digits
        password = "".join([secrets.choice(choices) for i in range(16)])
        return password

    def create_user(self, username: str, password: str) -> bool:
        """Create a user with given parameters.

        Args:
            username (str): Username of the user to be created.
            password (str): Password of the user to be created

        Returns:
            bool: signifies whether the user has been created successfully
        """
        self.logger.info(f"Creating user {username}...")
        query = "INSERT INTO kyuubi_users (username, passwd) VALUES (%s, %s);"
        vars = (username, password)
        status, _ = self.database.execute(
            query=query, vars=vars
        )
        return status

    def get_password(self, username: str) -> str:
        """Returns the password for the given username."""
        query = f"SELECT passwd FROM {AUTHENTICATION_TABLE_NAME} WHERE username = %s"
        vars = (username,)
        status, results = self.database.execute(
            query=query, vars=vars
        )
        if not status or len(results) == 0:
            raise Exception("Could not fetch password from authentication database.")
        password = results[0][0]
        return password

    def set_password(self, username: str, password: str) -> str:
        """Set a new password for the given username."""
        query = f"UPDATE {AUTHENTICATION_TABLE_NAME} SET passwd = %s WHERE username = %s"
        vars = (
            password,
            username,
        )
        status, _ = self.database.execute(
            query=query, vars=vars
        )
        if not status:
            raise Exception(f"Could not update password of {username}.")

    def prepare_auth_db(self) -> None:
        """Prepare the authentication database in PostgreSQL."""
        self.logger.info("Preparing auth db...")
        self.create_authentication_table()
        self.create_user(DEFAULT_ADMIN_USERNAME, self.generate_password())

    def remove_auth_db(self) -> None:
        """Remove authentication database from PostgreSQL."""
        self.logger.info("Removing auth_db...")
        query = f"DROP DATABASE {self.database.dbname} WITH (FORCE);"

        # Using POSTGRESQL_DEFAULT_DATABASE because a database can't be dropped
        # while being connected to itself.
        self.database.execute(dbname=POSTGRESQL_DEFAULT_DATABASE, query=query)
