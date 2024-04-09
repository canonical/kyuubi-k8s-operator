#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 connection utility classes and methods."""

from dataclasses import dataclass

import psycopg2

from constants import AUTHENTICATION_DATABASE_NAME
from utils import WithLogging
from database import DatabaseConnectionInfo


@dataclass
class AuthUtil(WithLogging):
    """Class representing credentials and endpoints to connect to Postgres database."""
    db_info: DatabaseConnectionInfo


    def execute_query(self, query: str) -> bool:
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(
                host=self.endpoint,
                user=self.username,
                password=self.password,
                dbname=AUTHENTICATION_DATABASE_NAME,
            )
            cursor = connection.cursor()
            cursor.execute(query=query)
            return True
        except Exception as e:
            self.logger.warning(f"PostgreSQL connection not successful. Reason: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
  

    def prepare_authentication_database(self) -> bool:
        """Create the authentication table in PostgreSQL database"""
        return self.execute_query("""
                CREATE TABLE kyuubi_users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(100) UNIQUE NOT NULL,
                    salt VARCHAR(100) NOT NULL,
                    password_hash VARCHAR(255) NOT NULL
                );

                DO $$
                DECLARE
                    salt TEXT := gen_salt('bf');
                BEGIN
                    INSERT INTO kyuubi_users (username, salt, password_hash) VALUES ('admin', salt, crypt('admin', salt));
                END $$;
            """)

    def remove_auth_db(self) -> bool:
        return self.execute_query(f"""
            DROP DATBASE {AUTHENTICATION_DATABASE_NAME} WITH (FORCE);
        """)

    