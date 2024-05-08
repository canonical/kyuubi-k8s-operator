# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Kyuubi client relation hooks & helpers."""

import logging

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseProvides,
    DatabaseRequestedEvent,
)
from ops.charm import CharmBase, RelationBrokenEvent
from ops.framework import Object
from ops.model import BlockedStatus

from managers.auth import Authentication

logger = logging.getLogger(__name__)


class KyuubiClientProvider(Object):
    """Defines functionality for the 'provides' side of the 'kyuubi-client' relation.

    Hook events observed:
        - database-requested
        - relation-broken
    """

    def __init__(self, charm: CharmBase, relation_name: str) -> None:
        """Constructor for KyuubiClientProvider object.

        Args:
            charm: the charm for which this relation is provided
            relation_name: the name of the relation
        """
        super().__init__(charm, relation_name)

        self.relation_name = relation_name
        self.charm = charm
        self.database_provides = DatabaseProvides(charm, relation_name)

        self.framework.observe(
            charm.on[self.relation_name].relation_broken, self._on_relation_broken
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

        if not self.charm.is_authentication_enabled():
            raise NotImplementedError(
                "Authentication has not been enabled yet! "
                "Please integrate kyuubi-k8s with postgresql-k8s "
                "over auth-db relation endpoint."
            )
        auth = Authentication(self.charm.auth_db_connection_info)
        try:
            username = f"relation_id_{event.relation.id}"
            password = auth.generate_password()
            auth.create_user(username=username, password=password)

            endpoint = self.charm.workload.get_jdbc_endpoint()

            # Set the read/write endpoint.
            self.database_provides.set_endpoints(
                event.relation.id,
                endpoint,
            )

            # Set the database version.
            self.database_provides.set_version(
                event.relation.id, self.charm.workload.kyuubi_version
            )
        except (Exception) as e:
            logger.exception(e)
            self.charm.unit.status = BlockedStatus(str(e))

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Remove the user created for this relation."""
        logger.info("KyuubiClientProvider: Relation broken...")

        if not self.charm.unit.is_leader():
            return

        auth = Authentication(self.charm.auth_db_connection_info)
        username = f"relation_id_{event.relation.id}"

        try:
            auth.delete_user(username)
        except Exception as e:
            logger.exception(e)
            self.charm.unit.status = BlockedStatus(
                f"Failed to delete user during {self.relation_name} relation broken event"
            )
