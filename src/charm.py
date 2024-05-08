#!/usr/bin/env -S LD_LIBRARY_PATH=lib python3
# The LD_LIBRARY_PATH variable needs to be set here because without that
# psycopg2 can't be imported due to missing libpq.so file (which is inside lib/)

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charm the Kyuubi service."""

import logging

import ops
from constants import (
    KYUUBI_CONTAINER_NAME,
)

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


from workload.kyuubi import KyuubiWorkload
from core.context import Context
from events.s3 import S3Events
from events.metastore import MetastoreEvents
from events.auth import AuthenticationEvents
from events.actions import ActionEvents


class KyuubiCharm(ops.CharmBase):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)

        self.workload = KyuubiWorkload(
            container=self.unit.get_container(KYUUBI_CONTAINER_NAME),
        )
        self.context = Context(self)

        self.s3 = S3Events(self, self.context, self.workload)
        self.metastore = MetastoreEvents(self, self.context, self.workload)
        self.auth = AuthenticationEvents(self, self.context, self.workload)
        self.actions = ActionEvents(self, self.context, self.workload)



if __name__ == "__main__":  # pragma: nocover
    ops.main(KyuubiCharm)  # type: ignore
