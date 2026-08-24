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

JOB_OCI_IMAGE = "ghcr.io/canonical/charmed-spark:3.5-22.04_edge@sha256:301e63491963c15d49cff8c8153c8ccbfc2c86ab869fc241d296ae2a588ce9bf"
GPU_JOB_OCI_IMAGE = "ghcr.io/canonical/charmed-spark-gpu:3.5-22.04_edge@sha256:dfdd68a2cfa90e14b6e8285e9b6b289997743af3d75706c6acfa814c5d1b87a3"

DEFAULT_ADMIN_USERNAME = "admin"
PASSWORD_SUFFIX = "-password"
ADMIN_PASSWORD_KEY = DEFAULT_ADMIN_USERNAME + PASSWORD_SUFFIX

# Zookeeper literals
HA_ZNODE_NAME = "/kyuubi"

# Literals related to metastore
HIVE_SCHEMA_VERSION = "2.3.0"

SECRETS_APP: list[str] = [ADMIN_PASSWORD_KEY]

TRUSTSTORE_SECRET_PREFIX = "integrator-hub-conf-truststore"
TRUSTSTORE_SECRET_NAME_KEY = "truststore_secret_name"

# Base directory under which Integration Hub mounts the S3 truststore secret in
# the Spark pods. Kept in sync with the integration hub so that Kyuubi (running
# as non-root `_daemon_`) can replicate the file for client-side operations.
HUB_TRUSTSTORE_MOUNT_BASE = "/etc/spark8t/conf"
