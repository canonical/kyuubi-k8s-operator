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

JOB_OCI_IMAGE = "ghcr.io/canonical/charmed-spark:4.0-22.04_edge@sha256:53efa8f18908512751b32f46656292418b7f1a6262e3957264739e7e54005ad8"
GPU_JOB_OCI_IMAGE = "ghcr.io/canonical/charmed-spark-gpu:4.0-22.04_edge@sha256:ad117ab42dc75851d8e44a455ab934268284ab5fdadd361a02b748db9eaa8f6a"

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
