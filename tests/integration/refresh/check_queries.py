#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Standalone connection/query smoke check for a live Kyuubi deployment.

This reproduces the "connect + run SQL" part of the refresh integration tests
(`test_create_new_data` / `test_validate_previous_data`) against an already
deployed model, so it can be run in CI to isolate query failures from the
rest of the refresh flow.

Examples:
    # JDBC auth, no TLS (default), against the current model
    python -m tests.integration.refresh.check_queries --model my-model

    # LDAP auth
    python -m tests.integration.refresh.check_queries --model my-model --ldap

    # TLS enabled (CA fetched from the self-signed-certificates app)
    python -m tests.integration.refresh.check_queries --model my-model --tls

    # Validate data written before an upgrade
    python -m tests.integration.refresh.check_queries --model my-model \
        --mode previous-data --db-name inplace_db --table-name inplace_table

Exit code is 0 on success, 1 on failure.
"""

from __future__ import annotations

import argparse
import ast
import json
import logging
import sys
import uuid

import jubilant
from spark_test.core.kyuubi import KyuubiClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("check_queries")

DEFAULT_APP = "kyuubi-k8s"
DEFAULT_DATA_INTEGRATOR = "data-integrator"
DEFAULT_TLS_APP = "self-signed-certificates"
DEFAULT_JDBC_PORT = 10009

# Mirrors tests/integration/helpers/auth.py
LDAP_TEST_USERNAME = "bikalpa"
LDAP_TEST_PASSWORD = "bikalpa"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default=None, help="Juju model (default: current model)")
    parser.add_argument("--app", default=DEFAULT_APP, help="Kyuubi application name")
    parser.add_argument(
        "--data-integrator",
        default=DEFAULT_DATA_INTEGRATOR,
        help="data-integrator application name (JDBC auth mode)",
    )
    parser.add_argument("--ldap", action="store_true", help="Use LDAP credentials")
    parser.add_argument("--username", default=None, help="Override username")
    parser.add_argument("--password", default=None, help="Override password")
    parser.add_argument("--tls", action="store_true", help="Connect using TLS")
    parser.add_argument("--tls-app", default=DEFAULT_TLS_APP, help="TLS provider application name")
    parser.add_argument(
        "--ca-cert", default=None, help="Path to a CA cert file (instead of fetching)"
    )
    parser.add_argument("--host", default=None, help="Override Kyuubi host")
    parser.add_argument("--port", type=int, default=None, help="Override Kyuubi port")
    parser.add_argument(
        "--mode",
        choices=["new-data", "previous-data"],
        default="new-data",
        help="new-data: create+insert+select; previous-data: read existing table",
    )
    parser.add_argument("--db-name", default=None, help="Database name (previous-data mode)")
    parser.add_argument("--table-name", default=None, help="Table name (previous-data mode)")
    return parser.parse_args(argv)


def fetch_connection_info(juju: jubilant.Juju, data_integrator: str) -> tuple[str, str, str]:
    """Return (uris, username, password) from the data-integrator get-credentials action."""
    logger.info("Running 'get-credentials' on %s/0", data_integrator)
    task = juju.run(f"{data_integrator}/0", "get-credentials")
    if task.return_code != 0:
        raise RuntimeError(f"get-credentials failed: {task.results}")
    kyuubi_info = task.results["kyuubi"]
    return kyuubi_info["uris"], kyuubi_info["username"], kyuubi_info["password"]


def fetch_ca_certificate(juju: jubilant.Juju, unit_name: str) -> str:
    """Fetch CA certificate from the self-signed-certificates operator."""
    logger.info("Fetching CA certificate from %s", unit_name)
    task = juju.run(unit_name, "get-issued-certificates")
    if task.return_code != 0:
        raise RuntimeError(f"get-issued-certificates failed: {task.results}")
    items = ast.literal_eval(task.results.get("certificates", "[]"))
    certificates = json.loads(items[0])
    return certificates.get("ca", "")


def host_port_from_uri(uri: str, default_port: int) -> tuple[str | None, int]:
    """Parse jdbc:hive2://host:port from a JDBC URI."""
    import re

    match = re.match(r"jdbc:hive2://([\w.\-]+):(\d+)", uri or "")
    if not match:
        return None, default_port
    return match.group(1), int(match.group(2))


def resolve_credentials(
    juju: jubilant.Juju, args: argparse.Namespace
) -> tuple[str, str, str | None]:
    """Return (username, password, uris)."""
    if args.username and args.password:
        return args.username, args.password, None
    if args.ldap:
        return (
            args.username or LDAP_TEST_USERNAME,
            args.password or LDAP_TEST_PASSWORD,
            None,
        )
    uris, username, password = fetch_connection_info(juju, args.data_integrator)
    return username, password, uris


def resolve_host_port(
    juju: jubilant.Juju, args: argparse.Namespace, uris: str | None
) -> tuple[str, int]:
    if args.host:
        return args.host, args.port or DEFAULT_JDBC_PORT
    if uris:
        host, port = host_port_from_uri(uris, args.port or DEFAULT_JDBC_PORT)
        if host:
            return host, port
    status = juju.status()
    host = status.apps[args.app].units[f"{args.app}/0"].address
    return host, args.port or DEFAULT_JDBC_PORT


def build_queries(args: argparse.Namespace) -> list[str]:
    if args.mode == "previous-data":
        if not args.db_name or not args.table_name:
            raise SystemExit("--db-name and --table-name are required for previous-data mode")
        return [
            f"USE `{args.db_name}`;",
            f"SELECT * FROM `{args.table_name}`;",
        ]
    db_name = args.db_name or str(uuid.uuid4()).replace("-", "_")
    table_name = args.table_name or str(uuid.uuid4()).replace("-", "_")
    return [
        f"CREATE DATABASE `{db_name}`;",
        f"USE `{db_name}`;",
        f"CREATE TABLE `{table_name}` (id INT);",
        f"INSERT INTO `{table_name}` VALUES (12345);",
        f"SELECT * FROM `{table_name}`;",
    ]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    juju = jubilant.Juju(model=args.model) if args.model else jubilant.Juju()

    username, password, uris = resolve_credentials(juju, args)
    host, port = resolve_host_port(juju, args, uris)

    ca_cert: str | None = None
    if args.tls:
        if args.ca_cert:
            ca_cert = args.ca_cert
        else:
            ca_cert = fetch_ca_certificate(juju, f"{args.tls_app}/0")

    logger.info(
        "Connecting to Kyuubi at %s:%s as user '%s' (tls=%s)", host, port, username, args.tls
    )

    client_args: dict = {"host": host, "port": int(port)}
    if username:
        client_args["username"] = username
    if password:
        client_args["password"] = password

    queries = build_queries(args)
    kyuubi_client = KyuubiClient(**client_args, use_ssl=args.tls, ca_cert=ca_cert)

    try:
        with kyuubi_client.connection as conn, conn.cursor() as cursor:
            for line in queries:
                logger.info("Executing: %s", line)
                cursor.execute(line)
            results = cursor.fetchall()
    except Exception:
        logger.exception("Query execution failed")
        return 1

    logger.info("Final result set: %s", results)
    if len(results) == 1:
        logger.info("SUCCESS: query check passed")
        return 0

    logger.error("FAILURE: expected exactly 1 row, got %d", len(results))
    return 1


if __name__ == "__main__":
    sys.exit(main())
