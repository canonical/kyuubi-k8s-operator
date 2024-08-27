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
S3_INTEGRATOR_REL = "s3-credentials"
POSTGRESQL_METASTORE_DB_REL = "metastore-db"
POSTGRESQL_AUTH_DB_REL = "auth-db"
SPARK_SERVICE_ACCOUNT_REL = "spark-service-account"
ZOOKEEPER_REL = "zookeeper"
KYUUBI_CLIENT_RELATION_NAME = "jdbc"

COS_METRICS_PORT = 10019
COS_METRICS_PATH = "/metrics"

# Literals related to K8s
NAMESPACE_CONFIG_NAME = "namespace"
SERVICE_ACCOUNT_CONFIG_NAME = "service-account"

# Literals related to Kyuubi
JDBC_PORT = 10009
KYUUBI_OCI_IMAGE = "ghcr.io/canonical/charmed-spark-kyuubi:3.4-22.04_edge"
DEFAULT_ADMIN_USERNAME = "admin"

# Zookeeper literals
HA_ZNODE_NAME = "/kyuubi"
HA_ZNODE_NAME_TEMP = "/kyuubi-temp"
