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
LDAP_RELATION_NAME = "ldap-credentials"
CERTIFICATES_TRANSFER_RELATION_NAME = "receive-ca-cert"
KYUUBI_CLIENT_RELATION_NAME = "jdbc"

COS_METRICS_PORT = 10019
COS_METRICS_PATH = "/metrics"
COS_LOG_RELATION_NAME_SERVER = "logging"

# Literals related to Kyuubi
JDBC_PORT = 10009
REST_PORT = 10099
SPARK_DEFAULT_CATALOG_NAME = "spark_catalog"

JOB_OCI_IMAGE = "ghcr.io/canonical/charmed-spark:3.4-22.04_edge@sha256:61c21596b21acdca64bce8fb7d6b8b9aaaad86e46ff6f30ac40c628ae7069bd3"  # 3.4-22.04 24-06-2026
GPU_JOB_OCI_IMAGE = "ghcr.io/canonical/charmed-spark-gpu:3.4-22.04_edge@sha256:d3db04e43561027381446b7a8a6edbfc10a10987e48c1ef26588f7a9bf8c3075"  # 3.4-22.04 24-06-2026

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
