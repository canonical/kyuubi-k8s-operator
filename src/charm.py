#!/usr/bin/env -S LD_LIBRARY_PATH=lib python3
# The LD_LIBRARY_PATH variable needs to be set here because without that
# psycopg2 can't be imported due to missing libpq.so file (which is inside lib/)

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charm the Kyuubi service."""

import logging
from typing import Optional

import ops
from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseCreatedEvent,
    DatabaseRequires,
)
from charms.data_platform_libs.v0.s3 import (
    CredentialsChangedEvent,
    CredentialsGoneEvent,
    S3Requirer,
)
from ops.charm import ActionEvent

from config.hive import HiveConfig
from config.kyuubi import KyuubiConfig
from config.spark import SparkConfig
from constants import (
    AUTHENTICATION_DATABASE_NAME,
    DEFAULT_ADMIN_USERNAME,
    KYUUBI_CLIENT_RELATION_NAME,
    KYUUBI_CONTAINER_NAME,
    METASTORE_DATABASE_NAME,
    NAMESPACE_CONFIG_NAME,
    POSTGRESQL_AUTH_DB_REL,
    POSTGRESQL_METASTORE_DB_REL,
    S3_INTEGRATOR_REL,
    SERVICE_ACCOUNT_CONFIG_NAME,
)
from database import DatabaseConnectionInfo
from models import Status
from relation import KyuubiClientProvider
from s3 import S3ConnectionInfo
from utils import k8s
from utils.auth import Authentication
from utils.io import IOMode
from workload import KyuubiServer

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class KyuubiCharm(ops.CharmBase):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)
        self.workload = KyuubiServer(self.unit.get_container(KYUUBI_CONTAINER_NAME))
        self.s3_requirer = S3Requirer(self, S3_INTEGRATOR_REL)
        self.metastore_db = DatabaseRequires(
            self, relation_name=POSTGRESQL_METASTORE_DB_REL, database_name=METASTORE_DATABASE_NAME
        )
        self.auth_db = DatabaseRequires(
            self,
            relation_name=POSTGRESQL_AUTH_DB_REL,
            database_name=AUTHENTICATION_DATABASE_NAME,
            extra_user_roles="superuser",
        )
        self.kyuubi_client_provider = KyuubiClientProvider(self, KYUUBI_CLIENT_RELATION_NAME)
        self.register_event_handlers()

    def register_event_handlers(self):
        """Register various event handlers to the charm."""
        self.framework.observe(self.on.install, self._update_event)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.kyuubi_pebble_ready, self._on_kyuubi_pebble_ready)
        self.framework.observe(self.on.update_status, self._update_event)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(
            self.s3_requirer.on.credentials_changed, self._on_s3_credential_changed
        )
        self.framework.observe(self.s3_requirer.on.credentials_gone, self._on_s3_credential_gone)
        self.framework.observe(
            self.metastore_db.on.database_created, self._on_metastore_db_created
        )
        self.framework.observe(
            self.metastore_db.on.endpoints_changed, self._on_metastore_db_created
        )
        self.framework.observe(
            self.on.metastore_db_relation_broken, self._on_metastore_db_relation_removed
        )
        self.framework.observe(self.auth_db.on.database_created, self._on_auth_db_created)
        self.framework.observe(
            self.auth_db.on.endpoints_changed, self._on_auth_db_endpoints_changed
        )
        self.framework.observe(self.on.auth_db_relation_broken, self._on_auth_db_relation_removed)
        self.framework.observe(
            self.on.auth_db_relation_departed, self._on_auth_db_relation_departed
        )
        self.framework.observe(self.on.get_jdbc_endpoint_action, self._on_get_jdbc_endpoint)
        self.framework.observe(self.on.get_password_action, self._on_get_password)
        self.framework.observe(self.on.set_password_action, self._on_set_password)

    def _on_install(self, event: ops.InstallEvent) -> None:
        """Handle the `on_install` event."""
        self.unit.status = Status.WAITING_PEBBLE.value

    def _on_metastore_db_created(self, event: DatabaseCreatedEvent) -> None:
        logger.info("Metastore database created...")
        self.update_service()

    def _on_metastore_db_relation_removed(self, event) -> None:
        logger.info("Mestastore database relation removed")
        self.update_service()

    def _on_auth_db_created(self, event: DatabaseCreatedEvent) -> None:
        logger.info("Authentication database created...")
        hostname, port = event.endpoints.split(":")
        db_connection_info = DatabaseConnectionInfo(
            endpoint=hostname,
            username=event.username,
            password=event.password,
            dbname=AUTHENTICATION_DATABASE_NAME,
        )
        Authentication(db_connection_info).prepare_auth_db()
        self.update_service()

    def _on_auth_db_endpoints_changed(self, event) -> None:
        logger.info("Authentication database endpoints changed...")
        self.update_service()

    def _on_auth_db_relation_removed(self, event) -> None:
        logger.info("Authentication database relation removed")
        self.update_service()

    def _on_auth_db_relation_departed(self, event) -> None:
        logger.info("Authentication database relation departed")
        Authentication(self.auth_db_connection_info).remove_auth_db()

    def _on_config_changed(self, event: ops.ConfigChangedEvent) -> None:
        """Handle the on_config_changed event."""
        if not self.unit.is_leader():
            return

        self.update_service()

    def _update_event(self, _):
        """Handle the update event hook."""
        self.unit.status = self.get_status()

    def _update_spark_configs(self):
        """Update Spark properties in the spark-defaults file inside the charm container."""
        s3_info = self.s3_connection_info
        metastore_db_info = self.metastore_db_connection_info
        auth_db_info = self.auth_db_connection_info
        namespace = self.config[NAMESPACE_CONFIG_NAME]
        service_account = self.config[SERVICE_ACCOUNT_CONFIG_NAME]
        with self.workload.get_spark_configuration_file(IOMode.WRITE) as spark_fid:
            config = SparkConfig(
                s3_info=s3_info, namespace=namespace, service_account=service_account
            )
            spark_fid.write(config.contents)
        with self.workload.get_hive_configuration_file(IOMode.WRITE) as hive_fid:
            config = HiveConfig(db_info=metastore_db_info)
            hive_fid.write(config.contents)
        with self.workload.get_kyuubi_configuration_file(IOMode.WRITE) as kyuubi_fid:
            config = KyuubiConfig(db_info=auth_db_info)
            kyuubi_fid.write(config.contents)

    def get_status(
        self,
    ) -> ops.StatusBase:
        """Compute and return the status of the charm."""
        if not self.workload.ready():
            return Status.WAITING_PEBBLE.value

        s3_info = self.s3_connection_info
        if not s3_info:
            return Status.MISSING_S3_RELATION.value

        if not s3_info.verify():
            return Status.INVALID_CREDENTIALS.value

        namespace = self.config[NAMESPACE_CONFIG_NAME]
        if not k8s.is_valid_namespace(namespace=namespace):
            return Status.INVALID_NAMESPACE.value

        service_account = self.config[SERVICE_ACCOUNT_CONFIG_NAME]
        if not k8s.is_valid_service_account(namespace=namespace, service_account=service_account):
            return Status.INVALID_SERVICE_ACCOUNT.value

        return Status.ACTIVE.value

    def update_service(
        self,
    ) -> bool:
        """Update the Kyuubi server service if needed."""
        # Set the unit status
        status = self.get_status()
        self.unit.status = status

        if status is not Status.ACTIVE.value:
            logger.info(f"Cannot start service because of status {status}")
            self.workload.stop()
            return False

        # Dynamically update the Spark properties
        self._update_spark_configs()

        # Start the workload
        self.workload.start()
        return True

    def _on_kyuubi_pebble_ready(self, event: ops.PebbleReadyEvent):
        """Define and start a workload using the Pebble API."""
        logger.info("Kyuubi pebble service is ready.")
        self.update_service()

    def _on_get_jdbc_endpoint(self, event: ActionEvent):
        result = {"endpoint": self.workload.get_jdbc_endpoint()}
        event.set_results(result)

    def _on_get_password(self, event: ActionEvent) -> None:
        """Returns the password for admin user."""
        if not self.is_authentication_enabled():
            event.fail(
                "The action can only be run when authentication is enabled. "
                "Please integrate kyuubi-k8s:auth-db with postgresql-k8s"
            )
            return
        password = Authentication(self.auth_db_connection_info).get_password(
            DEFAULT_ADMIN_USERNAME
        )
        event.set_results({"password": password})

    def _on_set_password(self, event: ActionEvent) -> None:
        """Set the password for the admin user."""
        if not self.is_authentication_enabled():
            event.fail(
                "The action can only be run when authentication is enabled. "
                "Please integrate kyuubi-k8s:auth-db with postgresql-k8s"
            )
            return
        # Only leader can write the new password
        if not self.unit.is_leader():
            event.fail("The action can be run only on leader unit")
            return

        auth = Authentication(self.auth_db_connection_info)
        password = auth.generate_password()

        if "password" in event.params:
            password = event.params["password"]

        if password == auth.get_password(DEFAULT_ADMIN_USERNAME):
            event.log("The old and new passwords are equal.")
            event.set_results({"password": password})
            return

        auth.set_password(DEFAULT_ADMIN_USERNAME, password)

        event.set_results({"password": password})

    @property
    def s3_connection_info(self) -> Optional[S3ConnectionInfo]:
        """Parse a S3ConnectionInfo object from relation data."""
        # If the relation is not yet available, return None
        if not self.s3_requirer.relations:
            return None

        raw_info = self.s3_requirer.get_s3_connection_info()

        return S3ConnectionInfo(
            endpoint=raw_info.get("endpoint"),
            access_key=raw_info.get("access-key"),
            secret_key=raw_info.get("secret-key"),
            path=raw_info.get("path"),
            bucket=raw_info.get("bucket"),
        )

    @property
    def metastore_db_connection_info(self) -> Optional[DatabaseConnectionInfo]:
        """Parse a DatabaseConnectionInfo object from metastore_db relation data."""
        # If the relation is not yet available, return None
        if not self.metastore_db.relations:
            return None

        raw_info = self.metastore_db.fetch_relation_data()
        for data in raw_info.values():
            if not data:
                continue
            return DatabaseConnectionInfo(
                endpoint=data["endpoints"],
                username=data["username"],
                password=data["password"],
                dbname=METASTORE_DATABASE_NAME,
            )
        return None

    @property
    def auth_db_connection_info(self) -> Optional[DatabaseConnectionInfo]:
        """Parse a DatabaseConnectionInfo object from metastore_db relation data."""
        # If the relation is not yet available, return None
        if not self.auth_db.relations:
            return None

        raw_info = self.auth_db.fetch_relation_data()
        for data in raw_info.values():
            if not data:
                continue
            hostname, port = data["endpoints"].split(":")
            return DatabaseConnectionInfo(
                endpoint=hostname,
                username=data["username"],
                password=data["password"],
                dbname=AUTHENTICATION_DATABASE_NAME,
            )
        return None

    def _on_s3_credential_changed(self, _: CredentialsChangedEvent):
        """Handle the `CredentialsChangedEvent` event from S3 integrator."""
        logger.info("S3 credentials changed")
        self.update_service()

    def _on_s3_credential_gone(self, _: CredentialsGoneEvent):
        """Handle the `CredentialsGoneEvent` event for S3 integrator."""
        logger.info("S3 credentials gone")
        self.update_service()

    def is_authentication_enabled(self) -> bool:
        """Returns whether the authentication has been enabled in the Kyuubi charm."""
        return self.auth_db.relations is not None


if __name__ == "__main__":  # pragma: nocover
    ops.main(KyuubiCharm)  # type: ignore
