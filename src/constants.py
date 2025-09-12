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

# spark 3.5.5, release date 12/09/2025
JOB_OCI_IMAGE = "ghcr.io/canonical/charmed-spark@sha256:13afe46fec2a84685e92c03fc94d96b52fbeee5c9494c5086537e6de86af512b"
# spark-gpu 3.5.5, release date 12/09/2025
GPU_JOB_OCI_IMAGE = "ghcr.io/canonical/charmed-spark-gpu@sha256:4ba140bc052089f4a4679e199e99c0f30b16bf6829b62e50bb3d8bef20e67872"

DEFAULT_ADMIN_USERNAME = "admin"
PASSWORD_SUFFIX = "-password"
ADMIN_PASSWORD_KEY = DEFAULT_ADMIN_USERNAME + PASSWORD_SUFFIX

# Zookeeper literals
HA_ZNODE_NAME = "/kyuubi"

# Literals related to metastore
HIVE_SCHEMA_VERSION = "2.3.0"

SECRETS_APP: list[str] = [ADMIN_PASSWORD_KEY]
