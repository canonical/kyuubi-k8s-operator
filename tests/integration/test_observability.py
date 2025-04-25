#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import yaml
from tenacity import Retrying, stop_after_attempt, wait_fixed

from .helpers import (
    all_prometheus_exporters_data,
    deploy_minimal_kyuubi_setup,
    get_cos_address,
    published_grafana_dashboards,
    published_loki_logs,
    published_prometheus_alerts,
    published_prometheus_data,
    validate_sql_queries_with_kyuubi,
)
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
COS_AGENT_APP_NAME = "grafana-agent-k8s"


def test_build_and_deploy(
    juju: jubilant.Juju,
    kyuubi_charm: Path,
    charm_versions: IntegrationTestsCharms,
    s3_bucket_and_creds: S3Info,
) -> None:
    """Deploy minimal Kyuubi deployments."""
    """Test the status of default managed K8s service when Kyuubi is deployed."""
    deploy_minimal_kyuubi_setup(
        juju=juju,
        kyuubi_charm=kyuubi_charm,
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=True,
        num_units=1,
        integrate_zookeeper=False,
    )

    # Wait for everything to settle down
    juju.wait(jubilant.all_active, delay=5)


def test_run_some_sql_queries(juju: jubilant.Juju) -> None:
    """Test running SQL queries without an external metastore."""
    assert validate_sql_queries_with_kyuubi(juju=juju)


def test_kyuubi_cos_monitoring_setup(juju: jubilant.Juju) -> None:
    """Setting up COS relations.

    This is important to happen before worker log files start to be generated.
    Only new logs will be picked up by Loki.
    """
    # Prometheus data is being published by the app
    assert all_prometheus_exporters_data(juju, check_field="kyuubi_jvm_uptime")

    # Deploying and relating to grafana-agent
    logger.info("Deploying grafana-agent-k8s charm...")
    juju.deploy(COS_AGENT_APP_NAME, num_units=1, base="ubuntu@22.04")

    logger.info("Waiting for test charm to be idle...")
    juju.wait(lambda status: jubilant.all_blocked(status, COS_AGENT_APP_NAME), delay=5)

    juju.integrate(COS_AGENT_APP_NAME, f"{APP_NAME}:metrics-endpoint")
    juju.integrate(COS_AGENT_APP_NAME, f"{APP_NAME}:grafana-dashboard")
    juju.integrate(COS_AGENT_APP_NAME, f"{APP_NAME}:logging")

    juju.deploy(
        "cos-lite",
        base="ubuntu@22.04",
        trust=True,
    )

    juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=10)
    juju.wait(lambda status: jubilant.all_blocked(status, COS_AGENT_APP_NAME), delay=10)

    juju.integrate(f"{COS_AGENT_APP_NAME}:grafana-dashboards-provider", "grafana")
    juju.integrate(f"{COS_AGENT_APP_NAME}:send-remote-write", "prometheus")
    juju.integrate(f"{COS_AGENT_APP_NAME}:logging-consumer", "loki")

    juju.wait(
        lambda status: jubilant.all_active(
            status, APP_NAME, COS_AGENT_APP_NAME, "prometheus", "alertmanager", "loki", "grafana"
        ),
        delay=10,
    )


def test_kyuubi_cos_data_published(juju: jubilant.Juju) -> None:
    # We should leave time for Prometheus data to be published
    for attempt in Retrying(stop=stop_after_attempt(10), wait=wait_fixed(60), reraise=True):
        with attempt:
            # Data got published to Prometheus
            logger.info("Checking if Prometheus data is being published...")
            cos_address = get_cos_address(juju)
            assert published_prometheus_data(juju, cos_address, "kyuubi_jvm_uptime")

            # Alerts got published to Prometheus
            logger.info("Checking if alert rules are published...")
            alerts_data = published_prometheus_alerts(juju, cos_address)

            for alert in ["KyuubiBufferPoolCapacityLow", "KyuubiJVMUptime"]:
                assert any(
                    rule["name"] == alert
                    for group in alerts_data["data"]["groups"]
                    for rule in group["rules"]
                )

            # Grafana dashboard got published
            logger.info("Checking the Kyuubi dashboard is available in Grafana...")
            dashboards_info = published_grafana_dashboards(juju)
            assert any(board["title"] == "Kyuubi" for board in dashboards_info)

            # Loki
            logger.info("Checking if Kyuubi server logs are published to Loki...")
            loki_server_logs = published_loki_logs(juju, "juju_application", "kyuubi-k8s", 5000)
            assert len(loki_server_logs["data"]["result"][0]["values"]) > 0

            # Ideally we should do the check below. However, this requires COS to be started
            # around application startup. Once this is possible, please un-comment the check below
            #
            # assert any(
            #     "Starting org.apache.kyuubi.server.KyuubiServer" in value[1]
            #     for result in loki_server_logs["data"]["result"]
            #     for value in result["values"]
            # )
