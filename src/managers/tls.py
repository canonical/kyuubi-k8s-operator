#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for building necessary files for Java TLS auth."""

import base64
import logging
import os
import re
import socket
import subprocess

import ops.pebble
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from core.context import Context
from core.domain import SANs, Secret
from core.enums import ExposeExternal
from core.workload import KyuubiWorkloadBase
from managers.service import DNSEndpoint, IPEndpoint

logger = logging.getLogger(__name__)


class TLSManager:
    """Manager for building necessary files for Java TLS auth."""

    def __init__(self, context: Context, workload: KyuubiWorkloadBase):
        self.context = context
        self.workload = workload

    @property
    def tls_private_key_secret(self) -> Secret:
        """Lazily initialize the system users Secret object."""
        secret_id = self.context.config.tls_client_private_key
        if secret_id is None:
            raise RuntimeError("TLS private key secret is not configured.")
        return Secret(model=self.context.model, secret_id=secret_id)

    def tls_private_key_secret_configured(self) -> bool:
        """Return whether the user configured has configured TLS private key secret."""
        return self.context.config.tls_client_private_key is not None

    def tls_private_key_secret_exists(self) -> bool:
        """Return whether the user configured TLS private key secret exists."""
        return self.tls_private_key_secret.exists()

    def tls_private_key_secret_granted(self) -> bool:
        """Return whether the user configured TLS private key secret has been granted to charm."""
        return self.tls_private_key_secret.has_permission()

    def tls_private_key_secret_valid(self) -> bool:
        """Return whether the user configured TLS private key secret is valid."""
        secret_content = self.tls_private_key_secret.content
        if not secret_content:
            return False
        if len(secret_content.keys()) != 1 or "private-key" not in secret_content:
            return False
        pkey: str = secret_content["private-key"]
        private_key = (
            pkey
            if re.match(r"(-+(BEGIN|END) [A-Z ]+-+)", pkey)
            else base64.b64decode(pkey).decode("utf-8").strip()
        )

        try:
            key = serialization.load_pem_private_key(
                private_key.encode(),
                password=None,
            )

            if not isinstance(key, rsa.RSAPrivateKey):
                logger.warning("Private key is not an RSA key")
                return False

            if key.key_size < 2048:
                logger.warning("RSA key size is less than 2048 bits")
                return False

            return True
        except ValueError:
            logger.warning("Invalid private key format")
            return False

    def update_tls_private_key(self) -> bool:
        """Update TLS private key in the Kyuubi cluster."""
        if (
            not self.tls_private_key_secret_configured()
            or not self.tls_private_key_secret_exists()
            or not self.tls_private_key_secret_granted()
            or not self.tls_private_key_secret_valid()
        ):
            private_key = ""
        else:
            key = self.tls_private_key_secret.content["private-key"]
            private_key = (
                key
                if re.match(r"(-+(BEGIN|END) [A-Z ]+-+)", key)
                else base64.b64decode(key).decode("utf-8")
            )

        if self.context.cluster.private_key == private_key:
            return False
        self.context.cluster.update({"private-key": private_key})
        return True

    def get_subject(self) -> str:
        """Get subject name for the unit."""
        if (
            self.context.config.expose_external.value == ExposeExternal.LOADBALANCER.value
            and (lb := self.context.unit_server.loadbalancer_endpoint) is not None
        ):
            return lb.host
        elif (
            self.context.config.expose_external.value == ExposeExternal.NODEPORT.value
            and (node_ip := self.context.unit_server.node_ip) != ""
        ):
            return node_ip

        return os.uname()[1]

    def build_sans(self) -> SANs:
        """Builds a SAN structure of DNS names and IPs for the unit."""
        sans_ip = [str(self.context.bind_address)]
        if node_ip := self.context.unit_server.node_ip:
            sans_ip.append(node_ip)

        match self.context.unit_server.loadbalancer_endpoint:
            case DNSEndpoint():
                # Do nothing, will be added to sans_dns anyway by 'external_address'
                # and was added to subject
                pass

            case IPEndpoint(host=host_ip):
                sans_ip.append(host_ip)

            case _:
                pass

        sans_dns = [
            self.context.unit_server.internal_address.split(".")[0],
            self.context.unit_server.internal_address,
            socket.getfqdn(),
        ]

        if (ext_address := self.context.unit_server.external_address) is not None:
            sans_dns.extend(
                [
                    f"{ext_address.host}:{ext_address.port}",
                    ext_address.host,
                ]
            )

        return SANs(
            sans_ip=sorted(sans_ip),
            sans_dns=sorted(sans_dns),
        )

    def get_current_sans(self) -> SANs | None:
        """Gets the current SANs for the unit cert."""
        if not self.context.unit_server.certificate:
            return None

        command = ["openssl", "x509", "-noout", "-ext", "subjectAltName", "-in", "server.pem"]
        try:
            sans_lines = self.workload.exec(
                command=" ".join(command), working_dir=str(self.workload.paths.conf_path)
            ).splitlines()
        except (subprocess.CalledProcessError, ops.pebble.ExecError) as e:
            logger.error(e.stdout)
            return None
        logger.info(f"sans line: {sans_lines}")
        for line in sans_lines:
            if "DNS" in line and "IP" in line:
                break
        sans_ip = []
        sans_dns = []
        for item in line.split(", "):
            san_type, san_value = item.split(":", maxsplit=1)
            if san_type.strip() == "DNS":
                sans_dns.append(san_value)
            if san_type.strip() == "IP Address":
                sans_ip.append(san_value)
        return SANs(sans_ip=sorted(sans_ip), sans_dns=sorted(sans_dns))

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
        except (subprocess.CalledProcessError, ops.pebble.ExecError) as import_ca_err:
            if "already exists" in str(import_ca_err.stdout):
                # Replacement strategy:
                # - We need to own the file, otherwise keytool throws a permission error upon removing an entry
                # - We need to make sure that the keystore is not empty at any point, hence the three steps.
                #  Otherwise, Kyuubi would pick up the file change when it's empty, and crash its internal watcher thread
                try:
                    self._rename_ca_in_truststore()
                    self._import_ca_in_truststore()
                    self._delete_ca_in_truststore()
                except ops.pebble.ExecError as e:
                    logger.error(str(e.stdout))
                    raise e

                return

            logger.error(str(import_ca_err.stdout))
            raise import_ca_err

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
                working_dir=str(self.workload.paths.conf_path),
            )
        except (subprocess.CalledProcessError, ops.pebble.ExecError) as e:
            logger.error(str(e.stdout))
            raise e
