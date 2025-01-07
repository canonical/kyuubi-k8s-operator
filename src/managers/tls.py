#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for building necessary files for Java TLS auth."""
import logging
import subprocess

import ops.pebble

from core.context import Context
from core.workload.kyuubi import KyuubiWorkload

logger = logging.getLogger(__name__)


class TLSManager:
    """Manager for building necessary files for Java TLS auth."""

    def __init__(self, context: Context, workload: KyuubiWorkload):
        self.context = context
        self.workload = workload

    def set_private_key(self) -> None:
        """Sets the unit private-key."""
        if not self.context.unit_server.private_key:
            logger.error("Can't set private-key to unit, missing private-key in relation data")
            return

        self.workload.write(
            content=self.context.unit_server.private_key, path=self.workload.paths.server_key
        )

    def set_ca(self) -> None:
        """Sets the unit CA."""
        if not self.context.unit_server.ca_cert:
            logger.error("Can't set CA to unit, missing CA in relation data")
            return

        self.workload.write(content=self.context.unit_server.ca_cert, path=self.workload.paths.ca)

    def set_certificate(self) -> None:
        """Sets the unit certificate."""
        if not self.context.unit_server.certificate:
            logger.error("Can't set certificate to unit, missing certificate in relation data")
            return

        self.workload.write(
            content=self.context.unit_server.certificate, path=self.workload.paths.certificate
        )

    def set_truststore(self) -> None:
        """Creates the unit Java Truststore and adds the unit CA."""
        try:
            self._import_ca_in_truststore()
        except (subprocess.CalledProcessError, ops.pebble.ExecError) as e:
            if "already exists" in str(e.stdout):
                # Replacement strategy:
                # - We need to own the file, otherwise keytool throws a permission error upon removing an entry
                # - We need to make sure that the keystore is not empty at any point, hence the three steps.
                #  Otherwise, ZK would pick up the file change when it's empty, and crash its internal watcher thread
                try:
                    self._rename_ca_in_truststore()
                    self._delete_ca_in_truststore()
                    self._import_ca_in_truststore()
                except (ops.pebble.ExecError) as e:

                    logger.error(str(e.stdout))
                    raise e

                return

            logger.error(str(e.stdout))
            raise e

    def _import_ca_in_truststore(self, alias: str = "ca") -> None:
        command = [
            "keytool",
            "-import",
            "-v",
            "-alias",
            alias,
            "-file",
            self.workload.paths.ca,
            "-keystore",
            self.workload.paths.truststore,
            "-storepass",
            self.context.unit_server.truststore_password,
            "-noprompt",
        ]
        self.workload.exec(" ".join(command))

    def _rename_ca_in_truststore(self, from_alias: str = "ca", to_alias: str = "old-ca") -> None:
        command = [
            "keytool",
            "-changealias",
            "-alias",
            from_alias,
            "-destalias",
            to_alias,
            "-keystore",
            self.workload.paths.truststore,
            "-storepass",
            self.context.unit_server.truststore_password,
        ]
        self.workload.exec(" ".join(command))

    def _delete_ca_in_truststore(self, alias: str = "old-ca") -> None:
        command = [
            "keytool",
            "-delete",
            "-v",
            "-alias",
            alias,
            "-keystore",
            self.workload.paths.truststore,
            "-storepass",
            self.context.unit_server.truststore_password,
        ]
        self.workload.exec(
            " ".join(command),
        )

    def set_p12_keystore(self) -> None:
        """Creates the unit Java Keystore and adds unit certificate + private-key."""
        command = [
            "openssl",
            "pkcs12",
            "-export",
            "-in",
            self.workload.paths.certificate,
            "-inkey",
            self.workload.paths.server_key,
            "-passin",
            f"pass:{self.context.unit_server.keystore_password}",
            "-certfile",
            self.workload.paths.certificate,
            "-out",
            self.workload.paths.keystore,
            "-password",
            f"pass:{self.context.unit_server.keystore_password}",
        ]
        try:
            self.workload.exec(
                " ".join(command),
            )
        except (subprocess.CalledProcessError, ops.pebble.ExecError) as e:
            logger.error(str(e.stdout))
            raise e

    def remove_stores(self) -> None:
        """Removes all certs, keys, stores from the unit."""
        command = [
            "rm",
            "-rf",
            self.workload.paths.ca,
            self.workload.paths.certificate,
            self.workload.paths.keystore,
            self.workload.paths.truststore,
        ]
        try:
            self.workload.exec(
                " ".join(command),
                working_dir=self.workload.paths.conf_path,  # type: ignore
            )
        except (subprocess.CalledProcessError, ops.pebble.ExecError) as e:
            logger.error(str(e.stdout))
            raise e
