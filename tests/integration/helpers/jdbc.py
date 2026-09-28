# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from __future__ import annotations

import logging
import re
import subprocess
import uuid
from pathlib import Path
from typing import cast

import jubilant
import yaml
from spark_test.core.kyuubi import KyuubiClient

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def get_random_name():
    return str(uuid.uuid4()).replace("-", "_")


def fetch_connection_info(juju: jubilant.Juju, data_integrator: str) -> tuple[str, str, str]:
    """Return the JDBC endpoint and credentials for clients to connect to Kyuubi server."""
    logger.info("Running action 'get-credentials' on data-integrator unit...")
    task = juju.run(
        f"{data_integrator}/0",
        "get-credentials",
    )
    assert task.return_code == 0
    kyuubi_info = task.results["kyuubi"]
    return kyuubi_info["uris"], kyuubi_info["username"], kyuubi_info["password"]


def run_sql_test_against_jdbc_endpoint(
    juju: jubilant.Juju, test_pod: str, jdbc_endpoint: str, username: str, password: str
) -> bool:
    """Verify the JDBC endpoint exposed by the charm with some SQL queries."""
    database_name = get_random_name()
    table_name = get_random_name()
    logger.info(
        "Testing JDBC endpoint by connecting with beeline and executing a few SQL queries. "
        f"Using database {database_name} and table {table_name} ..."
    )
    process = subprocess.run(
        [
            "./tests/integration/test_jdbc_endpoint.sh",
            test_pod,
            cast(str, juju.model),
            jdbc_endpoint,
            get_random_name(),
            get_random_name(),
            username,
            password,
        ],
        capture_output=True,
    )
    print("========== test_jdbc_endpoint.sh STDOUT =================")
    print(process.stdout.decode())
    print("========== test_jdbc_endpoint.sh STDERR =================")
    print(process.stderr.decode())
    logger.info(f"JDBC endpoint test returned with status {process.returncode}")
    return process.returncode == 0


def kyuubi_host_port_from_jdbc_uri(jdbc_uri: str) -> tuple[str, int]:
    pattern = r"jdbc:hive2://([\w\.-]+):(\d+)"
    match = re.match(pattern, jdbc_uri)
    if not match:
        raise ValueError(f"Invalid JDBC URI: {jdbc_uri}")
    host, port = match.groups()
    return host, int(port)


def validate_sql_queries_with_kyuubi(
    juju: jubilant.Juju,
    jdbc_uri: str | None = None,
    kyuubi_host: str | None = None,
    kyuubi_port: str | int = 10009,
    username: str | None = None,
    password: str | None = None,
    query_lines: list[str] | None = None,
    db_name: str | None = None,
    table_name: str | None = None,
    use_tls: bool = False,
    ca_cert: str | Path | None = None,
):
    """Run simple SQL queries to validate Kyuubi and return whether this validation is successful."""
    if jdbc_uri:
        kyuubi_host, kyuubi_port = kyuubi_host_port_from_jdbc_uri(jdbc_uri=jdbc_uri)
    if not kyuubi_host:
        kyuubi_host = juju.status().apps[APP_NAME].units[f"{APP_NAME}/0"].address
        logger.info(f"Reaching out to kyuubi on {kyuubi_host}")
    if not db_name:
        db_name = str(uuid.uuid4()).replace("-", "_")
    if not table_name:
        table_name = str(uuid.uuid4()).replace("-", "_")
    if not query_lines:
        query_lines = [
            f"CREATE DATABASE `{db_name}`; ",
            f"USE `{db_name}`; ",
            f"CREATE TABLE `{table_name}` (id INT); ",
            f"INSERT INTO `{table_name}` VALUES (12345); ",
            f"SELECT * FROM `{table_name}`; ",
        ]
    args = {"host": kyuubi_host, "port": int(kyuubi_port)}
    if username:
        args.update({"username": username})
    if password:
        args.update({"password": password})
    kyuubi_client = KyuubiClient(**args, use_ssl=use_tls, ca_cert=ca_cert)

    with kyuubi_client.connection as conn, conn.cursor() as cursor:
        for line in query_lines:
            cursor.execute(line)
        results = cursor.fetchall()
        return len(results) == 1
