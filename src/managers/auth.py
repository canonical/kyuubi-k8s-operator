#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Authentication manager."""

import secrets
import string

from constants import (
    AUTHENTICATION_TABLE_NAME,
    DEFAULT_ADMIN_USERNAME,
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
        self.database = DatabaseManager(db_info=context.auth_db)
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
    def system_user_password(self) -> str:
        """Return user configured system user password."""
        if (
            not self.system_user_secret_configured()
            or not self.system_user_secret_exists()
            or not self.system_user_secret_granted()
            or not self.system_user_secret_valid()
        ):
            return ""
        return self.system_users_secret.content.get(DEFAULT_ADMIN_USERNAME, "")

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
            CREATE TABLE IF NOT EXISTS {self.AUTHENTICATION_TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                username VARCHAR(100) UNIQUE NOT NULL,
                passwd TEXT NOT NULL
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
        query = f"INSERT INTO {self.AUTHENTICATION_TABLE_NAME} (username, passwd) VALUES (%s, crypt(%s, gen_salt('bf')) );"
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
        query = f"SELECT 1 FROM {self.AUTHENTICATION_TABLE_NAME} WHERE username = %s;"
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
        query = f"DELETE FROM {self.AUTHENTICATION_TABLE_NAME} WHERE username = %s;"
        vars = (username,)
        status, _ = self.database.execute(query=query, vars=vars)
        return status

    def set_password(self, username: str, password: str | None = None) -> bool:
        """Set a new password for the given username."""
        if password is None:
            password = self.generate_password()
        query = f"UPDATE {self.AUTHENTICATION_TABLE_NAME} SET passwd = crypt(%s, gen_salt('bf')) WHERE username = %s ;"
        vars = (
            password,
            username,
        )
        success, _ = self.database.execute(query=query, vars=vars)
        return success

    def create_admin_user(self) -> bool:
        """Create a default admin user in the authentication database."""
        password = self.system_user_password
        self.context.cluster.update({"admin-password": password})
        if not password:
            password = self.generate_password()
        return self.create_user(self.DEFAULT_ADMIN_USERNAME, password=password)

    def update_admin_user(self, force_update: bool = False) -> bool:
        """Update the default admin user password in the authentication database."""
        password = self.system_user_password
        should_update = True
        if password == self.context.cluster.admin_password:
            should_update = False
        self.context.cluster.update({"admin-password": password})
        if not force_update and not should_update:
            return True
        if not password:
            password = self.generate_password()
        return self.set_password(self.DEFAULT_ADMIN_USERNAME, password=password)

    def prepare_auth_db(self) -> bool:
        """Prepare the authentication database in PostgreSQL."""
        self.logger.info("Preparing auth db...")

        # TODO: this is to be done via configuration option from postgresql-k8s
        # in the future. We enable this here manually because postgresql-k8s
        # does not have config option to enable this extension yet.
        self.enable_pgcrypto_extension()

        self.create_authentication_table()
        if self.user_exists(self.DEFAULT_ADMIN_USERNAME):
            return self.update_admin_user(force_update=True)
        return self.create_admin_user()
