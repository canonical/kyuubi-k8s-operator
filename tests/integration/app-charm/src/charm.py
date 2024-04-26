#!/usr/bin/env python3
# Copyright 2024 Ubuntu
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk

"""Charm the service.

Refer to the following tutorial that will help you
develop a new k8s charm using the Operator Framework:

https://juju.is/docs/sdk/create-a-minimal-kubernetes-charm
"""


import logging

import ops
from charms.data_platform_libs.v0.data_interfaces import DatabaseRequires
from ops.model import ActiveStatus

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)

RELATION_NAME_KYUUBI = "jdbc"
DATABASE_NAME = "foobar"


class ApplicationCharm(ops.CharmBase):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)
        self.kyuubi_requirer = DatabaseRequires(
            self, relation_name=RELATION_NAME_KYUUBI, database_name=DATABASE_NAME
        )
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.kyuubi_requirer.on.database_created, self._on_database_created)
        self.framework.observe(self.on.jdbc_relation_broken, self._on_jdbc_relation_broken)

    def _on_start(self, event):
        self.unit.status = ActiveStatus("")

    def _on_database_created(self, event):
        logger.info(
            f"Database created with username={event.username} and password={event.password}..."
        )

    def _on_jdbc_relation_broken(self, event):
        logger.info("JDBC relation removed...")


if __name__ == "__main__":  # pragma: nocover
    ops.main(ApplicationCharm)  # type: ignore
