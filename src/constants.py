# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Literals and constants."""

KYUUBI_CONTAINER_NAME = "kyuubi"
KYUUBI_SERVICE_NAME = "kyuubi"
METASTORE_DATABASE_NAME = "hivemetastore"
AUTHENTICATION_DATABASE_NAME = "auth_db"
AUTHENTICATION_TABLE_NAME = "kyuubi_users"
POSTGRESQL_DEFAULT_DATABASE = "postgres"

S3_INTEGRATOR_REL = "s3-credentials"
POSTGRESQL_METASTORE_DB_REL = "metastore-db"
POSTGRESQL_AUTH_DB_REL = "auth-db"
SPARK_SERVICE_ACCOUNT_REL = "spark-service-account"

NAMESPACE_CONFIG_NAME = "namespace"
SERVICE_ACCOUNT_CONFIG_NAME = "service-account"

JDBC_PORT = 10009

KYUUBI_OCI_IMAGE = "ghcr.io/canonical/charmed-spark-kyuubi:3.4-22.04_edge"

DEFAULT_ADMIN_USERNAME = "admin"
KYUUBI_CLIENT_RELATION_NAME = "jdbc"
