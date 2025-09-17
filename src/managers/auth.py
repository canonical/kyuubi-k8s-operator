#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Authentication manager."""

import secrets
import string

from constants import AUTHENTICATION_TABLE_NAME, DEFAULT_ADMIN_USERNAME
from core.domain import DatabaseConnectionInfo
from managers.database import DatabaseManager
from utils.logging import WithLogging


class AuthenticationManager(WithLogging):
    """Manager encapsulating various authentication related methods."""

    def __init__(self, db_info: DatabaseConnectionInfo) -> None:
        super().__init__()
        self.database = DatabaseManager(db_info=db_info)

    def enable_pgcrypto_extension(self) -> bool:
        """Enable pgcrypto extension in the authentication database."""
        self.logger.info("Enabling pgcrypto extension...")
        query = "CREATE EXTENSION IF NOT EXISTS pgcrypto;"
        status, _ = self.database.execute(query)
        if not status:
            raise RuntimeError("Could not enable pgcrypto extension.")
        return status

    def create_authentication_table(self) -> bool:
        """Create authentication table in the authentication database."""
        self.logger.info("Creating authentication table...")
        query = f"""
            CREATE TABLE IF NOT EXISTS {AUTHENTICATION_TABLE_NAME} (
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
        query = f"INSERT INTO {AUTHENTICATION_TABLE_NAME} (username, passwd) VALUES (%s, crypt(%s, gen_salt('bf')) );"
        vars = (username, password)
        success, _ = self.database.execute(query=query, vars=vars)
        return success

    def user_exists(self, username: str) -> bool:
        """Check whether the user with given username already exists.

        Args:
            username (str): Username of the user.

        Returns:
            bool: signifies whether the user already exists
        """
        query = f"SELECT 1 FROM {AUTHENTICATION_TABLE_NAME} WHERE username = %s;"
        vars = (username,)
        success, result = self.database.execute(query=query, vars=vars)
        if not success:
            self.logger.error(f"Could not check if user {username} exists.")
            return False
        return len(result) != 0

    def delete_user(self, username: str) -> bool:
        """Delete a user with given username.

        Args:
            username (str): Username of the user to be deleted.

        Returns:
            bool: signifies whether the user has been deleted successfully
        """
        self.logger.info(f"Deleting user {username}...")
        query = f"DELETE FROM {AUTHENTICATION_TABLE_NAME} WHERE username = %s;"
        vars = (username,)
        status, _ = self.database.execute(query=query, vars=vars)
        return status

    def set_password(self, username: str, password: str) -> bool:
        """Set a new password for the given username."""
        query = f"UPDATE {AUTHENTICATION_TABLE_NAME} SET passwd = crypt(%s, gen_salt('bf')) WHERE username = %s ;"
        vars = (
            password,
            username,
        )
        success, _ = self.database.execute(query=query, vars=vars)
        return success

    def prepare_auth_db(self, admin_password: str) -> bool:
        """Prepare the authentication database in PostgreSQL."""
        self.logger.info("Preparing auth db...")

        # TODO: this is to be done via configuration option from postgresql-k8s
        # in the future. We enable this here manually because postgresql-k8s
        # does not have config option to enable this extension yet.
        self.enable_pgcrypto_extension()

        self.create_authentication_table()
        if self.user_exists(DEFAULT_ADMIN_USERNAME):
            return self.set_password(DEFAULT_ADMIN_USERNAME, password=admin_password)

        return self.create_user(DEFAULT_ADMIN_USERNAME, password=admin_password)
