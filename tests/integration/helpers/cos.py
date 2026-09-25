# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from __future__ import annotations

import json
import logging
import urllib.request
from pathlib import Path
from typing import cast
from urllib.parse import urlencode

import jubilant
import requests
import yaml
from tenacity import Retrying, stop_after_attempt, wait_fixed

from constants import COS_METRICS_PORT

from ..types import IntegrationTestsCharms, TelemetryAgent

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def _get_cos_address(juju: jubilant.Juju) -> str:
    """Retrieve the URL where COS services are available."""
    task = juju.run("traefik/0", "show-proxied-endpoints")
    assert task.return_code == 0
    endpoints = task.results["proxied-endpoints"]
    return json.loads(endpoints)["traefik"]["url"]


def _get_grafana_access(juju: jubilant.Juju) -> tuple[str, str]:
    """Get Grafana URL and password."""
    task = juju.run("grafana/0", "get-admin-password")
    assert task.return_code == 0
    return task.results["url"], task.results["admin-password"]


def _get_prometheus_exporter_data(host: str) -> str | None:
    """Check if a given host has metric service available and it is publishing."""
    url = f"http://{host}:{COS_METRICS_PORT}/metrics"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        return None
    if response.status_code == 200:
        return response.text
    return None


def get_logs_in_loki(juju: jubilant.Juju, filter_by_label: dict[str, str]):
    """Retrieve logs from Loki for a specific application filtered by labels."""
    base_url = _get_cos_address(juju)
    loki_address = f"{base_url}/{cast(str, juju.model)}-loki-0"

    try:
        response = json.loads(urllib.request.urlopen(f"{loki_address}/loki/api/v1/labels").read())
    except Exception:
        response = {}
    assert "success" == response["status"], "Failed to get labels from Loki"
    labels = response["data"]
    for key in filter_by_label:
        assert key in labels, f"Log label '{key}' not found in Loki labels: {labels}"

    for key, value in filter_by_label.items():
        try:
            response = json.loads(
                urllib.request.urlopen(f"{loki_address}/loki/api/v1/label/{key}/values").read()
            )
        except Exception:
            response = {}
        logger.info(f"Response for values for key '{key}': {response}")
        assert "success" == response["status"]
        assert value in response["data"][0], (
            f"Expected value '{value}' for label '{key}' not found in Loki"
        )

    # check for history server logs in loki
    url = f"{loki_address}/loki/api/v1/query_range"
    query = ",".join([f'{key}="{value}"' for key, value in filter_by_label.items()])
    keys = {"query": f"{{{query}}}"}
    data = urlencode(keys).encode()

    try:
        response = json.loads(urllib.request.urlopen(url, data).read().decode())
        logger.info(response)
    except Exception:
        response = {}

    assert "success" == response["status"], (
        f"Failed to query Loki; query used: {query}, received response: {response}"
    )
    assert "stream" in response["data"]["result"][0]
    for key, value in filter_by_label.items():
        assert value == response["data"]["result"][0]["stream"].get(key), (
            f"Expected value '{value}' for label '{key}' not found in Loki stream"
        )

    logs = response["data"]["result"][0]["values"]
    logger.info(f"Retrieved logs: {logs}")
    return logs


def assert_prometheus_data_exported(
    juju: jubilant.Juju,
    check_field: str = "kyuubi_jvm_uptime",
) -> None:
    """Assert that Prometheus data for the specified field is exported by all units."""
    result = True
    status = juju.status()
    for unit in status.apps[APP_NAME].units.values():
        unit_ip = unit.address
        result = result and check_field in (_get_prometheus_exporter_data(unit_ip) or "")
    assert result is True, f"Prometheus data for field '{check_field}' not exported by all units"


def assert_prometheus_data_published(
    juju: jubilant.Juju,
    check_field: str = "kyuubi_jvm_uptime",
) -> None:
    """Assert that Prometheus data for the specified field is published."""
    # We should leave time for Prometheus data to be published
    cos_address = _get_cos_address(juju)
    if "http://" in cos_address:
        cos_address = cos_address.split("//")[1]
    url = f"http://{cos_address}/{cast(str, juju.model)}-prometheus-0/api/v1/query?query={check_field}"
    for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_fixed(30), reraise=True):
        with attempt:
            # Data got published to Prometheus
            response = requests.get(url).json()
            assert "data" in response, (
                f"Prometheus query for field '{check_field}' failed: {response}"
            )
            assert "result" in response["data"], (
                f"Prometheus query for field '{check_field}' returned no results: {response}"
            )
            assert len(response["data"]["result"]) > 0, (
                f"Prometheus query for field '{check_field}' returned empty result: {response}"
            )


def assert_prometheus_alerts_published(
    juju: jubilant.Juju,
) -> None:
    """Assert that Prometheus alerts are published."""
    # We should leave time for Prometheus data to be published
    cos_address = _get_cos_address(juju)
    if "http://" in cos_address:
        cos_address = cos_address.split("//")[1]
    url = f"http://{cos_address}/{cast(str, juju.model)}-prometheus-0/api/v1/rules"
    for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_fixed(30), reraise=True):
        with attempt:
            # Alerts got published to Prometheus
            response = requests.get(url).json()
            assert response is not None
            assert "data" in response, f"Prometheus query for alerts failed: {response}"
            assert "groups" in response["data"], (
                f"Prometheus query for alerts returned no groups: {response}"
            )
            assert len(response["data"]["groups"]) > 0, (
                f"Prometheus query for alerts returned empty groups: {response}"
            )

            for alert in [
                "KyuubiMissing",
                "KyuubiHighAvailability",
            ]:
                assert any(
                    rule["name"] == alert
                    for group in response["data"]["groups"]
                    for rule in group["rules"]
                ), f"Prometheus query for alert '{alert}' returned no matching rules: {response}"


def assert_grafana_dashboards_published(
    juju: jubilant.Juju,
) -> None:
    """Assert that Grafana dashboards are published."""
    base_url, pw = _get_grafana_access(juju)
    url = f"{base_url}/api/search?query=&starred=false"
    for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_fixed(30), reraise=True):
        with attempt:
            session = requests.Session()
            session.auth = ("admin", pw)
            response = session.get(url).json()
            assert response is not None, f"Failed to get Grafana dashboards: {response}"
            assert any(board["title"] == "Kyuubi" for board in response), (
                f"Grafana dashboard 'Kyuubi' not found in response: {response}"
            )


def assert_logs_published_in_loki(
    juju: jubilant.Juju, filter_by_label: dict[str, str], search_phrase: str = ""
) -> None:
    """Assert that logs containing the specified search phrase are published in Loki."""
    for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_fixed(10), reraise=True):
        with attempt:
            logs = get_logs_in_loki(juju=juju, filter_by_label=filter_by_label)
            assert len(logs) > 0, f"No logs found with labels '{filter_by_label}'"

            c = len([log_line for log_line in logs if search_phrase in log_line[1]])
            assert c > 0, (
                f"No logs found containing the phrase '{search_phrase}' with labels '{filter_by_label}' Logs: {logs}"
            )


def deploy_observability_setup(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    telemetry_agent: TelemetryAgent = TelemetryAgent.GRAFANA_AGENT,
) -> None:
    """Deploy the observability setup including the specified telemetry agent."""
    logger.info(f"Deploying {telemetry_agent} charm...")
    telemetry_agent_charm = (
        charm_versions.grafana_agent
        if telemetry_agent == TelemetryAgent.GRAFANA_AGENT
        else charm_versions.otel_collector
    )
    juju.deploy(**telemetry_agent_charm.deploy_dict())

    logger.info("Waiting for the charms to be idle...")
    juju.wait(jubilant.all_agents_idle, delay=15)

    juju.integrate(telemetry_agent_charm.application_name, f"{APP_NAME}:metrics-endpoint")
    juju.integrate(telemetry_agent_charm.application_name, f"{APP_NAME}:grafana-dashboard")
    juju.integrate(telemetry_agent_charm.application_name, f"{APP_NAME}:logging")
    juju.wait(jubilant.all_agents_idle, delay=15)
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=15)

    juju.deploy("cos-lite", trust=True)
    juju.wait(jubilant.all_agents_idle, delay=15)

    juju.integrate(
        f"{telemetry_agent_charm.application_name}:grafana-dashboards-provider", "grafana"
    )
    juju.integrate(f"{telemetry_agent_charm.application_name}:send-remote-write", "prometheus")
    juju.integrate(f"{telemetry_agent_charm.application_name}", "loki:logging")

    juju.wait(jubilant.all_active, delay=20, timeout=600)
    logger.info("Observability setup deployed successfully.")
