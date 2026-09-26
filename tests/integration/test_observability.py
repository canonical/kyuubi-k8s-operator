#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import yaml

from .helpers.cos import (
    assert_grafana_dashboards_published,
    assert_logs_published_in_loki,
    assert_prometheus_alerts_published,
    assert_prometheus_data_exported,
    assert_prometheus_data_published,
    deploy_observability_setup,
)
from .helpers.jdbc import fetch_connection_info, validate_sql_queries_with_kyuubi
from .helpers.kyuubi import deploy_minimal_kyuubi_setup
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
    juju.wait(jubilant.all_active, delay=15)


def test_run_some_sql_queries(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    """Test running SQL queries without an external metastore."""
    _, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)

    assert validate_sql_queries_with_kyuubi(juju=juju, username=username, password=password)


def test_kyuubi_cos_monitoring_setup(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
) -> None:
    """Setting up COS relations.

    This is important to happen before worker log files start to be generated.
    Only new logs will be picked up by Loki.
    """
    deploy_observability_setup(juju, charm_versions=charm_versions)
    juju.wait(jubilant.all_active, delay=5)


def test_kyuubi_cos_data_published(juju: jubilant.Juju) -> None:
    """Test that COS data is published correctly."""
    assert_prometheus_data_exported(juju, check_field="kyuubi_jvm_uptime")
    assert_prometheus_data_published(juju, check_field="kyuubi_jvm_uptime")
    assert_prometheus_alerts_published(juju)
    assert_grafana_dashboards_published(juju)
    assert_logs_published_in_loki(juju, filter_by_label={"juju_application": "kyuubi-k8s"})
