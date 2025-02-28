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

KYUUBI_OCI_IMAGE = "ghcr.io/welpaolo/charmed-spark@sha256:cd16bae5b8c718724a84dbe8ee10fd28a9c5dfbcf504b7767b8e4f5507135113"
DEFAULT_ADMIN_USERNAME = "admin"

# Zookeeper literals
HA_ZNODE_NAME = "/kyuubi"

# Upgrade policies

DEPENDENCIES = {
    "service": {
        "dependencies": {},
        "name": "kyuubi",
        "upgrade_supported": "^1",
        "version": "1.9",
    },
}

SECRETS_APP = []
