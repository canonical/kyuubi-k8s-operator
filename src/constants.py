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
REST_PORT = 10099
SPARK_DEFAULT_CATALOG_NAME = "spark_catalog"

# spark 3.5.5, release date 16/01/2026
JOB_OCI_IMAGE = "ghcr.io/canonical/charmed-spark@sha256:a21e7b2d697fe3cfc61986ac40e60f4e9b6e72c4c2b37b34d3e2a6f46a66a80c"
# spark-gpu 3.5.5, release date 16/01/2026
GPU_JOB_OCI_IMAGE = "ghcr.io/canonical/charmed-spark-gpu@sha256:06f62f7361f1916aa9f7d27b48428277db690371d2d1acc0dd4ff2674ab75114"

DEFAULT_ADMIN_USERNAME = "admin"
PASSWORD_SUFFIX = "-password"
ADMIN_PASSWORD_KEY = DEFAULT_ADMIN_USERNAME + PASSWORD_SUFFIX

# Zookeeper literals
HA_ZNODE_NAME = "/kyuubi"

# Literals related to metastore
HIVE_SCHEMA_VERSION = "2.3.0"

SECRETS_APP: list[str] = [ADMIN_PASSWORD_KEY]
