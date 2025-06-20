# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Kyuubi client relation hooks & helpers."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseProvides,
    DatabaseRequestedEvent,
)
from ops.charm import RelationBrokenEvent
from ops.model import BlockedStatus

from constants import KYUUBI_CLIENT_RELATION_NAME
from core.context import Context
from core.workload.kyuubi import KyuubiWorkload
from events.base import BaseEventHandler
from managers.auth import AuthenticationManager
from managers.service import ServiceManager
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KyuubiCharm

logger = logging.getLogger(__name__)


class KyuubiClientProviderEvents(BaseEventHandler, WithLogging):
    """Defines event hooks for the provider side of the 'kyuubi-client' relation.

    Hook events observed:
        - database-requested
        - relation-broken
    """

    def __init__(self, charm: KyuubiCharm, context: Context, workload: KyuubiWorkload):
        super().__init__(charm, "kyuubi-client-provider")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.database_provides = DatabaseProvides(charm, KYUUBI_CLIENT_RELATION_NAME)

        self.framework.observe(
            charm.on[KYUUBI_CLIENT_RELATION_NAME].relation_broken, self._on_relation_broken
        )
        self.framework.observe(
            self.database_provides.on.database_requested, self._on_database_requested
        )

    def update_clients_endpoints(self) -> None:
        """Update related clients.

        Used if expose-external or TLS is changed
        """
        if not self.charm.unit.is_leader():
            return

        service_manager = ServiceManager(
            namespace=self.charm.model.name,
            unit_name=self.charm.unit.name,
            app_name=self.charm.app.name,
        )

        kyuubi_endpoint = service_manager.get_service_endpoint(
            expose_external=self.charm.config.expose_external
        )
        if kyuubi_endpoint is None:
            return

        # FIXME: What about nodeports? Same in _on_database_requested
        jdbc_uri = f"jdbc:hive2://{kyuubi_endpoint.host}:{kyuubi_endpoint.port}/"
        endpoint = f"{kyuubi_endpoint.host}:{kyuubi_endpoint.port}"

        for client in self.context.client_relations:
            self.database_provides.set_endpoints(
                client.id,
                endpoint,
            )

            self.database_provides.set_uris(client.id, jdbc_uri)
            self.database_provides.set_version(client.id, self.workload.kyuubi_version)
            self.database_provides.set_tls(
                client.id, "True" if self.context.cluster.tls else "False"
            )
            if self.context.cluster.tls:
                self.database_provides.set_tls_ca(client.id, self.context.unit_server.ca_cert)
            else:
                self.database_provides.set_tls_ca(client.id, "")

    def _on_database_requested(self, event: DatabaseRequestedEvent) -> None:
        """Handle the database-requested event.

        Generate a user and password for the related application.
        """
        logger.info("KyuubiClientProvider: Database requested...")

        if not self.context.is_authentication_enabled() or event.database is None:
            event.defer()
            return

        auth = AuthenticationManager(self.context)
        service_manager = ServiceManager(
            namespace=self.charm.model.name,
            unit_name=self.charm.unit.name,
            app_name=self.charm.app.name,
        )

        kyuubi_endpoint = service_manager.get_service_endpoint(
            expose_external=self.charm.config.expose_external
        )

        if kyuubi_endpoint is None:
            event.defer()
            return

        username = f"relation_id_{event.relation.id}"
        password = auth.generate_password()
        if not auth.create_user(username=username, password=password):
            logging.warning("User could not be created; deferring")
            event.defer()
            return

        jdbc_uri = f"jdbc:hive2://{kyuubi_endpoint.host}:{kyuubi_endpoint.port}/"

        # Set the JDBC endpoint.
        self.database_provides.set_endpoints(
            event.relation.id,
            f"{kyuubi_endpoint.host}:{kyuubi_endpoint.port}",
        )

        # Set the JDBC URI
        self.database_provides.set_uris(event.relation.id, jdbc_uri)

        # Set the database version.
        self.database_provides.set_version(event.relation.id, self.workload.kyuubi_version)

        # Set username and password
        self.database_provides.set_credentials(
            relation_id=event.relation.id, username=username, password=password
        )

        self.database_provides.set_database(event.relation.id, event.database)
        self.database_provides.set_tls(
            event.relation.id, "True" if self.context.cluster.tls else "False"
        )
        if self.context.cluster.tls:
            self.database_provides.set_tls_ca(event.relation.id, self.context.unit_server.ca_cert)

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Remove the user created for this relation."""
        logger.info("KyuubiClientProvider: Relation broken...")

        if not self.charm.unit.is_leader():
            return

        # FIXME: There is not guarantee here
        auth = AuthenticationManager(self.context)
        username = f"relation_id_{event.relation.id}"

        try:
            auth.delete_user(username)
        except Exception as e:
            logger.exception(e)
            self.charm.unit.status = BlockedStatus(
                f"Failed to delete user during {KYUUBI_CLIENT_RELATION_NAME} relation broken event"
            )
