#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for building necessary files for Java TLS auth."""

import logging
import os
import socket
import subprocess

import ops.pebble
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization

from core.context import Context
from core.domain import SANs
from core.workload import KyuubiWorkloadBase
from managers.service import DNSEndpoint, IPEndpoint

logger = logging.getLogger(__name__)

KYUUBI_SERVER_CA_ALIAS = "kyuubi-server-ca"


class TLSManager:
    """Manager for building necessary files for Java TLS auth."""

    SUBJECT_NAME_MAX_LENGTH = 64

    def __init__(self, context: Context, workload: KyuubiWorkloadBase):
        self.context = context
        self.workload = workload

    def get_kyuubi_subject_name(self) -> str:
        """Get subject name for the unit."""
        if external_address := self.context.unit_server.external_address:
            subject_name = external_address.host
        else:
            subject_name = os.uname()[1]
        if len(subject_name) > self.SUBJECT_NAME_MAX_LENGTH:
            logger.warning(
                f"The subject name {subject_name} is {len(subject_name)} characters long. "
                f"Using only first {self.SUBJECT_NAME_MAX_LENGTH} characters."
            )
            subject_name = subject_name[: self.SUBJECT_NAME_MAX_LENGTH]
        return subject_name

    def build_kyuubi_sans(self) -> SANs:
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
        if not self.context.unit_server.kyuubi_server_certificate:
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

    def set_kyuubi_server_ca(self) -> None:
        """Sets the unit CA."""
        if not self.context.unit_server.kyuubi_server_ca_cert:
            logger.error("Can't set CA to unit, missing CA in relation data")
            return

        self.workload.write(
            content=self.context.unit_server.kyuubi_server_ca_cert,
            path=self.workload.paths.kyuubi_server_ca,
        )

    def set_kyuubi_server_certificate(self) -> None:
        """Sets the unit certificate."""
        if not self.context.unit_server.kyuubi_server_certificate:
            logger.error("Can't set certificate to unit, missing certificate in relation data")
            return

        self.workload.write(
            content=self.context.unit_server.kyuubi_server_certificate,
            path=self.workload.paths.kyuubi_server_certificate,
        )

    def set_transferred_certificates(self, relation_id: int) -> None:
        """Sets the unit transferred certificates."""
        if not self.context.unit_server.get_transferred_certificates_for_relation(
            relation_id=relation_id
        ):
            logger.error(
                "Can't set transferred certificates to unit, missing certificates in certificate_transfer relation data"
            )
            return

        self.workload.write(
            content=self.context.unit_server.get_transferred_certificates_for_relation(
                relation_id=relation_id
            ),
            path=self.workload.paths.transferred_certificate_file(relation_id=relation_id),
        )

    def get_transferred_unit_certificates(self, relation_id: int) -> list[x509.Certificate]:
        """Gets the unit transferred certificates."""
        if not self.workload.exists(
            self.workload.paths.transferred_certificate_file(relation_id=relation_id)
        ):
            logger.error(
                "Can't get transferred certificates from unit, missing transferred certificates file"
            )
            return []

        bundle_bytes = self.workload.read(
            self.workload.paths.transferred_certificate_file(relation_id=relation_id)
        ).encode()
        certificates = x509.load_pem_x509_certificates(bundle_bytes)
        return certificates

    def generate_alias_for_certificate(self, certificate: x509.Certificate) -> str:
        """Generates an alias for the given certificate based on its SHA256 fingerprint."""
        fingerprint = certificate.fingerprint(hashes.SHA256()).hex()[:16]
        return f"transferred-cert-{fingerprint}"

    def set_transferred_certificates_truststore(self, relation_id: int) -> None:
        """Creates the unit Java Truststore and adds the transferred certificates."""
        for certificate in self.get_transferred_unit_certificates(relation_id=relation_id):
            alias = self.generate_alias_for_certificate(certificate)
            with self.workload.temporary_file(
                content=certificate.public_bytes(encoding=serialization.Encoding.PEM).decode(
                    "utf-8"
                ),
                mode="w",
            ) as cert_path:
                self.import_certificate(
                    alias=alias,
                    cert_path=cert_path,
                    truststore_path=self.workload.paths.truststore,
                    truststore_password=self.context.unit_server.truststore_password,
                )

    def set_kyuubi_server_truststore(self) -> None:
        """Creates the unit Java Truststore and adds the unit CA."""
        self.import_certificate(
            alias=KYUUBI_SERVER_CA_ALIAS,
            cert_path=self.workload.paths.kyuubi_server_ca,
            truststore_path=self.workload.paths.truststore,
            truststore_password=self.context.unit_server.truststore_password,
        )

    def set_kyuubi_server_p12_keystore(self) -> None:
        """Creates the unit Java Keystore and adds unit certificate + private-key."""
        command = [
            "openssl",
            "pkcs12",
            "-export",
            "-in",
            self.workload.paths.kyuubi_server_certificate,
            "-inkey",
            self.workload.paths.server_key,
            "-passin",
            f"pass:{self.context.unit_server.keystore_password}",
            "-certfile",
            self.workload.paths.kyuubi_server_certificate,
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

    def delete_kyuubi_server_certificate(self) -> None:
        """Delete Kyuubi server certificate."""
        self._delete_cert_from_truststore(
            alias=KYUUBI_SERVER_CA_ALIAS,
            truststore_path=self.workload.paths.truststore,
            truststore_password=self.context.unit_server.truststore_password,
        )
        self.workload.delete(self.workload.paths.kyuubi_server_certificate, recursive=True)
        self.workload.delete(self.workload.paths.kyuubi_server_ca, recursive=True)
        self.workload.delete(self.workload.paths.keystore, recursive=True)

    def delete_transferred_certificates(self, relation_id: int) -> None:
        """Delete the transferred certificates for given relation ID."""
        certificates = self.get_transferred_unit_certificates(relation_id=relation_id)
        for certificate in certificates:
            alias = self.generate_alias_for_certificate(certificate)
            self._delete_cert_from_truststore(
                alias=alias,
                truststore_path=self.workload.paths.truststore,
                truststore_password=self.context.unit_server.truststore_password,
            )
        self.workload.delete(
            self.workload.paths.transferred_certificate_file(relation_id=relation_id),
            recursive=True,
        )

    def _import_cert_into_truststore(
        self, alias: str, cert_path: str, truststore_path: str, truststore_password: str
    ) -> None:
        command = [
            "keytool",
            "-import",
            "-v",
            "-alias",
            alias,
            "-file",
            cert_path,
            "-keystore",
            truststore_path,
            "-storepass",
            truststore_password,
            "-noprompt",
        ]
        self.workload.exec(" ".join(command))

    def _rename_cert_in_truststore(
        self, from_alias: str, to_alias: str, truststore_path: str, truststore_password: str
    ) -> None:
        command = [
            "keytool",
            "-changealias",
            "-alias",
            from_alias,
            "-destalias",
            to_alias,
            "-keystore",
            truststore_path,
            "-storepass",
            truststore_password,
        ]
        self.workload.exec(" ".join(command))

    def _delete_cert_from_truststore(
        self, alias: str, truststore_path: str, truststore_password: str
    ) -> None:
        command = [
            "keytool",
            "-delete",
            "-v",
            "-alias",
            alias,
            "-keystore",
            truststore_path,
            "-storepass",
            truststore_password,
        ]
        self.workload.exec(
            " ".join(command),
        )

    def import_certificate(
        self, alias: str, cert_path: str, truststore_path: str, truststore_password: str
    ) -> None:
        """Import the given certificate into the given truststore, replacing it if it already exists.

        By design, this method is supposed to be idempotent, meaning that if the certificate already exists in the truststore,
        it will be replaced with the new one.
        """
        try:
            self._import_cert_into_truststore(
                alias=alias,
                cert_path=cert_path,
                truststore_path=truststore_path,
                truststore_password=truststore_password,
            )
        except (subprocess.CalledProcessError, ops.pebble.ExecError) as import_cert_err:
            if "already exists" in str(import_cert_err.stdout):
                # Replacement strategy:
                # - We need to own the file, otherwise keytool throws a permission error upon removing an entry
                # - We need to make sure that the truststore is not empty at any point, hence the three steps.
                #  Otherwise, Kyuubi would pick up the file change when it's empty, and crash its internal watcher thread
                try:
                    self._rename_cert_in_truststore(
                        from_alias=alias,
                        to_alias=f"old-{alias}",
                        truststore_path=truststore_path,
                        truststore_password=truststore_password,
                    )
                    self._import_cert_into_truststore(
                        alias=alias,
                        cert_path=cert_path,
                        truststore_path=truststore_path,
                        truststore_password=truststore_password,
                    )
                    self._delete_cert_from_truststore(
                        alias=f"old-{alias}",
                        truststore_path=truststore_path,
                        truststore_password=truststore_password,
                    )
                except ops.pebble.ExecError as e:
                    logger.error(str(e.stdout))
                    raise e

                return

            logger.error(str(import_cert_err.stdout))
            raise import_cert_err
