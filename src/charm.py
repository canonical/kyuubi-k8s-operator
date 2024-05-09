#!/usr/bin/env -S LD_LIBRARY_PATH=lib python3
# The LD_LIBRARY_PATH variable needs to be set here because without that
# psycopg2 can't be imported due to missing libpq.so file (which is inside lib/)

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charm the Kyuubi service."""

import logging

import ops
from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseRequires,
)
from charms.data_platform_libs.v0.s3 import (
    S3Requirer,
)

from constants import (
    AUTHENTICATION_DATABASE_NAME,
    KYUUBI_CLIENT_RELATION_NAME,
    KYUUBI_CONTAINER_NAME,
    METASTORE_DATABASE_NAME,
    POSTGRESQL_AUTH_DB_REL,
    POSTGRESQL_METASTORE_DB_REL,
    S3_INTEGRATOR_REL,
)
from core.context import Context
from events.actions import ActionEvents
from events.auth import AuthenticationEvents
from events.kyuubi import KyuubiEvents
from events.metastore import MetastoreEvents
from events.s3 import S3Events
from providers import KyuubiClientProvider
from workload.kyuubi import KyuubiWorkload

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class KyuubiCharm(ops.CharmBase):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)

        # Workload
        self.workload = KyuubiWorkload(
            container=self.unit.get_container(KYUUBI_CONTAINER_NAME),
        )

        # Context
        self.context = Context(self)

        # Requirers
        self.metastore_db = DatabaseRequires(
            self, relation_name=POSTGRESQL_METASTORE_DB_REL, database_name=METASTORE_DATABASE_NAME
        )
        self.auth_db = DatabaseRequires(
            self,
            relation_name=POSTGRESQL_AUTH_DB_REL,
            database_name=AUTHENTICATION_DATABASE_NAME,
            extra_user_roles="superuser",
        )
        self.s3 = S3Requirer(self, S3_INTEGRATOR_REL)

        # Providers
        self.kyuubi_client = KyuubiClientProvider(self, KYUUBI_CLIENT_RELATION_NAME)

        # Event handlers
        self.kyuubi_events = KyuubiEvents(self, self.context, self.workload)
        self.s3_events = S3Events(self, self.context, self.workload)
        self.metastore_events = MetastoreEvents(self, self.context, self.workload)
        self.auth_events = AuthenticationEvents(self, self.context, self.workload)
        self.action_events = ActionEvents(self, self.context, self.workload)


if __name__ == "__main__":  # pragma: nocover
    ops.main(KyuubiCharm)  # type: ignore
