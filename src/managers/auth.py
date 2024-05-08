#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Authentication manager."""

import secrets
import string

from constants import (
    POSTGRESQL_DEFAULT_DATABASE,
)
from database import DatabaseConnectionInfo
from utils.logging import WithLogging
from managers.database import DatabaseManager

class AuthenticationManager(WithLogging):
    """Manager encapsulating various authentication related methods."""

    DEFAULT_ADMIN_USERNAME = "admin"
    AUTHENTICATION_TABLE_NAME = "kyuubi_users"

    def __init__(self, db_info: DatabaseConnectionInfo = None) -> None:
        super().__init__()
        self.database = DatabaseManager(db_info=db_info)

    def create_authentication_table(self) -> bool:
        """Create authentication table in the authentication database."""
        self.logger.info("Creating authentication table...")
        query = f"""
            CREATE TABLE {self.AUTHENTICATION_TABLE_NAME} (
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
        query = f"INSERT INTO {self.AUTHENTICATION_TABLE_NAME} (username, passwd) VALUES (%s, %s);"
        vars = (username, password)
        status, _ = self.database.execute(query=query, vars=vars)
        return status

    def delete_user(self, username: str) -> bool:
        """Delete a user with given username.

        Args:
            username (str): Username of the user to be deleted.

        Returns:
            bool: signifies whether the user has been deleted successfully
        """
        self.logger.info(f"Deleting user {username}...")
        query = f"DELETE FROM {self.AUTHENTICATION_TABLE_NAME} WHERE username = %s;"
        vars = (username,)
        status, _ = self.database.execute(query=query, vars=vars)
        return status

    def get_password(self, username: str) -> str:
        """Returns the password for the given username."""
        query = f"SELECT passwd FROM {self.AUTHENTICATION_TABLE_NAME} WHERE username = %s"
        vars = (username,)
        status, results = self.database.execute(query=query, vars=vars)
        if not status or len(results) == 0:
            raise Exception("Could not fetch password from authentication database.")
        password = results[0][0]
        return password

    def set_password(self, username: str, password: str) -> str:
        """Set a new password for the given username."""
        query = f"UPDATE {self.AUTHENTICATION_TABLE_NAME} SET passwd = %s WHERE username = %s"
        vars = (
            password,
            username,
        )
        status, _ = self.database.execute(query=query, vars=vars)
        if not status:
            raise Exception(f"Could not update password of {username}.")

    def create_admin_user(self) -> bool:
        """Create a default admin user in the authentication database."""
        password = self.generate_password()
        return self.create_user(self.DEFAULT_ADMIN_USERNAME, password)

    def prepare_auth_db(self) -> None:
        """Prepare the authentication database in PostgreSQL."""
        self.logger.info("Preparing auth db...")
        self.create_authentication_table()
        self.create_admin_user()

    def remove_auth_db(self) -> None:
        """Remove authentication database from PostgreSQL."""
        self.logger.info("Removing auth_db...")
        query = f"DROP DATABASE {self.database.dbname} WITH (FORCE);"

        # Using POSTGRESQL_DEFAULT_DATABASE because a database can't be dropped
        # while being connected to itself.
        self.database.execute(dbname=POSTGRESQL_DEFAULT_DATABASE, query=query)
