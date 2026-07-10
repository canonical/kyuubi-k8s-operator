#!/usr/bin/env python3

# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

"""Kyuubi environment variables."""

from core.domain import SparkServiceAccountInfo, TLSInfo
from utils.logging import WithLogging


class KyuubiEnvironConfig(WithLogging):
    """Kyuubi Environment Variables."""

    def __init__(
        self,
        service_account_info: SparkServiceAccountInfo | None,
        backend_tls_info: TLSInfo | None,
        truststore_path: str,
    ):
        self.service_account_info = service_account_info
        self.backend_tls_info = backend_tls_info
        self.truststore_path = truststore_path

    def _base_env(self) -> dict[str, str]:
        """Return base environment variables."""
        if not self.service_account_info:
            return {}
        spark_extra_java_options = self.service_account_info.spark_properties.get(
            "spark.driver.extraJavaOptions"
        )
        if not spark_extra_java_options:
            return {}

        # SPARK_SUBMIT_OPTS should have truststore properties for Kyuubi to be able to
        # upload the Kyuubi engine JAR to Object Storage
        # We currently reuse the spark.driver.extraJavaOptions for this purpose.
        # TODO: Consider finding better alternatives to do this, if any.
        return {"SPARK_SUBMIT_OPTS": spark_extra_java_options}

    def _tls_env(self) -> dict[str, str]:
        """Return TLS related environment variables."""
        if not self.backend_tls_info:
            return {}

        return {
            "KYUUBI_JAVA_OPTS": (
                f"-Djavax.net.ssl.trustStore={self.truststore_path} "
                f"-Djavax.net.ssl.trustStorePassword={self.backend_tls_info.truststore_password}"
            )
        }

    def to_dict(self) -> dict[str, str]:
        """Return the dict representation of the configuration file."""
        return self._base_env() | self._tls_env()

    @property
    def contents(self) -> str:
        """Return configuration contents formatted to be consumed by pebble layer."""
        dict_content = self.to_dict()

        return "\n".join(
            [
                f'export {key}="{value}"'
                for key in sorted(dict_content.keys())
                if (value := dict_content[key])
            ]
        )
