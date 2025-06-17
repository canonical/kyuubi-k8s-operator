#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Authentication manager."""

import secrets
import string

from constants import (
    AUTHENTICATION_TABLE_NAME,
    DEFAULT_ADMIN_USERNAME,
    POSTGRESQL_DEFAULT_DATABASE,
)
from core.context import Context
from core.domain import Secret
from managers.database import DatabaseManager
from utils.logging import WithLogging


class AuthenticationManager(WithLogging):
    """Manager encapsulating various authentication related methods."""

    DEFAULT_ADMIN_USERNAME = DEFAULT_ADMIN_USERNAME
    AUTHENTICATION_TABLE_NAME = AUTHENTICATION_TABLE_NAME

    def __init__(self, context: Context) -> None:
        super().__init__()
        self.context = context
        db_info = context.auth_db
        self.database = DatabaseManager(db_info=db_info)
        self.system_users_secret = Secret(
            model=context.model, secret_id=context.config.system_users
        )

    def system_user_secret_configured(self) -> bool:
        """Return whether user has configured the system-users secret."""
        self.logger.error(self.context.config.system_users)
        return bool(self.context.config.system_users)

    def system_user_secret_exists(self) -> bool:
        """Return whether user-configured system users secret exists."""
        return self.system_users_secret.exists()

    def system_user_secret_granted(self) -> bool:
        """Return whether user-configured system users secret has been granted to the charm."""
        return self.system_users_secret.has_permission()

    def system_user_secret_valid(self) -> bool:
        """Return whether user-configured system users secret is valid."""
        secret_content = self.system_users_secret.content
        if not secret_content:
            return False
        admin_password = secret_content.get(DEFAULT_ADMIN_USERNAME)
        if admin_password in (None, ""):
            return False
        return True

    @property
    def system_user_password(self) -> str | None:
        """Return user configured system user password."""
        if (
            not self.system_user_secret_configured()
            or not self.system_user_secret_exists()
            or not self.system_user_secret_granted()
            or not self.system_user_secret_valid()
        ):
            return None

        return self.system_users_secret.content.get(DEFAULT_ADMIN_USERNAME)

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

    def user_exists(self, username: str) -> bool:
        """Check whether the user with given username already exists.

        Args:
            username (str): Username of the user.

        Returns:
            bool: signifies whether the user already exists
        """
        query = f"SELECT 1 FROM {self.AUTHENTICATION_TABLE_NAME} WHERE username = %s;"
        vars = (username,)
        success, result = self.database.execute(query=query, vars=vars)
        if not success:
            self.logger.error(f"Could not check if user {username} exists.")
            return False
        return len(result) != 0

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

    def set_password(self, username: str, password: str | None = None) -> bool:
        """Set a new password for the given username."""
        if password is None:
            password = self.generate_password()
        query = f"UPDATE {self.AUTHENTICATION_TABLE_NAME} SET passwd = %s WHERE username = %s"
        vars = (
            password,
            username,
        )
        status, _ = self.database.execute(query=query, vars=vars)
        if not status:
            raise Exception(f"Could not update password of {username}.")
        return status

    def create_admin_user(self) -> bool:
        """Create a default admin user in the authentication database."""
        password = self.system_user_password
        if password is None:
            password = self.generate_password()
        return self.create_user(self.DEFAULT_ADMIN_USERNAME, password=password)

    def update_admin_user(self) -> bool:
        """Update the default admin user password in the authentication database."""
        password = self.system_user_password
        if password is None:
            password = self.generate_password()
        return self.set_password(self.DEFAULT_ADMIN_USERNAME, password=password)

    def prepare_auth_db(self) -> None:
        """Prepare the authentication database in PostgreSQL."""
        self.logger.info("Preparing auth db...")
        self.create_authentication_table()
        self.create_admin_user()

    def remove_auth_db(self) -> None:
        """Remove authentication database from PostgreSQL."""
        self.logger.info("Removing auth_db...")
        query = f"DROP DATABASE {self.database.db_info.dbname} WITH (FORCE);"

        # Using POSTGRESQL_DEFAULT_DATABASE because a database can't be dropped
        # while being connected to itself.
        self.database.execute(dbname=POSTGRESQL_DEFAULT_DATABASE, query=query)
