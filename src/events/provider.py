# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Kyuubi client relation hooks & helpers."""

import logging

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseProvides,
    DatabaseRequestedEvent,
)
from charms.data_platform_libs.v0.data_models import TypedCharmBase
from ops.charm import RelationBrokenEvent
from ops.model import BlockedStatus

from constants import KYUUBI_CLIENT_RELATION_NAME
from core.context import Context
from core.workload import KyuubiWorkloadBase
from events.base import BaseEventHandler
from managers.auth import AuthenticationManager
from managers.service import ServiceManager
from utils.logging import WithLogging

logger = logging.getLogger(__name__)


class KyuubiClientProviderEvents(BaseEventHandler, WithLogging):
    """Defines event hooks for the provider side of the 'kyuubi-client' relation.

    Hook events observed:
        - database-requested
        - relation-broken
    """

    def __init__(self, charm: TypedCharmBase, context: Context, workload: KyuubiWorkloadBase):
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

    def _on_database_requested(self, event: DatabaseRequestedEvent) -> None:
        """Handle the database-requested event.

        Generate a user and password for the related application.
        """
        logger.info("KyuubiClientProvider: Database requested...")

        if not self.charm.unit.is_leader():
            return

        if not self.context.is_authentication_enabled():
            raise NotImplementedError(
                "Authentication has not been enabled yet! "
                "Please integrate kyuubi-k8s with postgresql-k8s "
                "over auth-db relation endpoint."
            )
        auth = AuthenticationManager(self.context.auth_db)
        service_manager = ServiceManager(
            namespace=self.charm.model.name,
            unit_name=self.charm.unit.name,
            app_name=self.charm.app.name,
        )
        try:
            username = f"relation_id_{event.relation.id}"
            password = auth.generate_password()
            auth.create_user(username=username, password=password)

            kyuubi_address = service_manager.get_service_endpoint(
                expose_external=self.charm.config.expose_external
            )
            endpoint = f"jdbc:hive2://{kyuubi_address}/" if kyuubi_address else ""

            # Set the JDBC endpoint.
            self.database_provides.set_endpoints(
                event.relation.id,
                endpoint,
            )

            # Set the database version.
            self.database_provides.set_version(event.relation.id, self.workload.kyuubi_version)

            # Set username and password
            self.database_provides.set_credentials(
                relation_id=event.relation.id, 
                username=username, 
                password=password
            )

            self.database_provides.set_database(event.relation.id, event.database)
            self.database_provides.set_tls(event.relation.id, "True" if self.context.cluster.tls else "False")
            if self.context.cluster.tls:
                self.database_provides.set_tls_ca(event.relation.id, self.context.unit_server.ca_cert)

        except (Exception) as e:
            logger.exception(e)
            self.charm.unit.status = BlockedStatus(str(e))

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Remove the user created for this relation."""
        logger.info("KyuubiClientProvider: Relation broken...")

        if not self.charm.unit.is_leader():
            return

        auth = AuthenticationManager(self.context.auth_db)
        username = f"relation_id_{event.relation.id}"

        try:
            auth.delete_user(username)
        except Exception as e:
            logger.exception(e)
            self.charm.unit.status = BlockedStatus(
                f"Failed to delete user during {KYUUBI_CLIENT_RELATION_NAME} relation broken event"
            )
