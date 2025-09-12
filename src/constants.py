# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Literals and constants."""

KYUUBI_CONTAINER_NAME = "kyuubi"
KYUUBI_SERVICE_NAME = "kyuubi"

# Database related literals
METASTORE_DATABASE_NAME = "hivemetastore"
AUTHENTICATION_DATABASE_NAME = "auth_db"
AUTHENTICATION_TABLE_NAME = "kyuubi_users"
POSTGRESQL_DEFAULT_DATABASE = "postgres"

# Relation names
PEER_REL = "kyuubi-peers"
POSTGRESQL_METASTORE_DB_REL = "metastore-db"
POSTGRESQL_AUTH_DB_REL = "auth-db"
SPARK_SERVICE_ACCOUNT_REL = "spark-service-account"
ZOOKEEPER_REL = "zookeeper"
TLS_REL = "certificates"
KYUUBI_CLIENT_RELATION_NAME = "jdbc"

COS_METRICS_PORT = 10019
COS_METRICS_PATH = "/metrics"
COS_LOG_RELATION_NAME_SERVER = "logging"

# Literals related to Kyuubi
JDBC_PORT = 10009
SPARK_DEFAULT_CATALOG_NAME = "spark_catalog"

# spark 3.4.4, release date 11/09/2025
JOB_OCI_IMAGE = "ghcr.io/canonical/charmed-spark@sha256:c4e9b0a9404e9b5caff24d7937b30f5228da12b1a2c75e6126c4856619f39972"
# spark-gpu 3.4.4, release date 11/09/2025
GPU_JOB_OCI_IMAGE = "ghcr.io/canonical/charmed-spark-gpu@sha256:eb8200b96e43584df7d9b49030c74fceef29246b976a6ac11d7412c64765f9f8"

DEFAULT_ADMIN_USERNAME = "admin"
PASSWORD_SUFFIX = "-password"
ADMIN_PASSWORD_KEY = DEFAULT_ADMIN_USERNAME + PASSWORD_SUFFIX

# Zookeeper literals
HA_ZNODE_NAME = "/kyuubi"

# Literals related to metastore
HIVE_SCHEMA_VERSION = "2.3.0"

SECRETS_APP: list[str] = [ADMIN_PASSWORD_KEY]
