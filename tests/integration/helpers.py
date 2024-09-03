#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import json
from pathlib import Path
from subprocess import PIPE, check_output

import requests
import yaml
from pytest_operator.plugin import OpsTest

from constants import COS_METRICS_PORT

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
OPENSEARCH_APP_NAME = "opensearch"


def prometheus_exporter_data(host: str) -> str | None:
    """Check if a given host has 'kibana-exporter' service available and publishing."""
    url = f"http://{host}:{COS_METRICS_PORT}/metrics"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        return
    if response.status_code == 200:
        return response.text


async def all_prometheus_exporters_data(ops_test: OpsTest, check_field) -> bool:
    """Check if a all units has 'kibana-exporter' service available and publishing."""
    result = True
    for unit in ops_test.model.applications[APP_NAME].units:
        unit_ip = await get_address(ops_test, unit.name)
        result = result and check_field in prometheus_exporter_data(unit_ip)
    return result


def published_prometheus_alerts(ops_test: OpsTest, host: str) -> dict | None:
    """Retrieve all Prometheus Alert rules that have been published."""
    if "http://" in host:
        host = host.split("//")[1]
    url = f"http://{host}/{ops_test.model.name}-prometheus-0/api/v1/rules"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        return

    if response.status_code == 200:
        return response.json()


def published_prometheus_data(ops_test: OpsTest, host: str, field: str) -> dict | None:
    """Check the existence of field among Prometheus published data."""
    if "http://" in host:
        host = host.split("//")[1]
    url = f"http://{host}/{ops_test.model.name}-prometheus-0/api/v1/query?query={field}"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        return

    if response.status_code == 200:
        return response.json()


async def published_grafana_dashboards(ops_test: OpsTest) -> str | None:
    """Get the list of dashboards published to Grafana."""
    base_url, pw = await get_grafana_access(ops_test)
    url = f"{base_url}/api/search?query=&starred=false"

    try:
        session = requests.Session()
        session.auth = ("admin", pw)
        response = session.get(url)
    except requests.exceptions.RequestException:
        return
    if response.status_code == 200:
        return response.json()


async def get_cos_address(ops_test: OpsTest) -> str:
    """Retrieve the URL where COS services are available."""
    cos_addr_res = check_output(
        f"JUJU_MODEL={ops_test.model.name} juju run traefik/0 show-proxied-endpoints --format json",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )

    try:
        cos_addr = json.loads(cos_addr_res)
    except json.JSONDecodeError:
        raise ValueError

    endpoints = cos_addr["traefik/0"]["results"]["proxied-endpoints"]
    return json.loads(endpoints)["traefik"]["url"]


async def get_grafana_access(ops_test: OpsTest) -> tuple[str, str]:
    """Get Grafana URL and password."""
    grafana_res = check_output(
        f"JUJU_MODEL={ops_test.model.name} juju run grafana/0 get-admin-password --format json",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )

    try:
        grafana_data = json.loads(grafana_res)
    except json.JSONDecodeError:
        raise ValueError

    url = grafana_data["grafana/0"]["results"]["url"]
    password = grafana_data["grafana/0"]["results"]["admin-password"]
    return url, password


async def get_address(ops_test: OpsTest, unit_name: str) -> str:
    """Get the address for a unit."""
    status = await ops_test.model.get_status()  # noqa: F821
    address = status["applications"][APP_NAME]["units"][f"{unit_name}"]["address"]
    return address
