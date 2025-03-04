#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import juju
import pytest
import yaml
from pytest_operator.plugin import OpsTest
from tenacity import Retrying, stop_after_attempt, wait_fixed

from core.domain import Status

from .helpers import (
    all_prometheus_exporters_data,
    check_status,
    deploy_minimal_kyuubi_setup,
    get_cos_address,
    published_grafana_dashboards,
    published_loki_logs,
    published_prometheus_alerts,
    published_prometheus_data,
    run_sql_test_against_jdbc_endpoint,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
TEST_CHARM_PATH = "./tests/integration/app-charm"
TEST_CHARM_NAME = "application"
COS_AGENT_APP_NAME = "grafana-agent-k8s"


@pytest.mark.skip_if_deployed
@pytest.mark.abort_on_fail
async def test_build_and_deploy(
    ops_test: OpsTest, kyuubi_charm, charm_versions, s3_bucket_and_creds
):
    """Deploy minimal Kyuubi deployments."""
    """Test the status of default managed K8s service when Kyuubi is deployed."""
    await deploy_minimal_kyuubi_setup(
        ops_test=ops_test,
        kyuubi_charm=kyuubi_charm,
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=True,
        num_units=1,
        integrate_zookeeper=False,
    )

    # Wait for everything to settle down
    await ops_test.model.wait_for_idle(
        apps=[
            APP_NAME,
            charm_versions.integration_hub.application_name,
            charm_versions.s3.application_name,
        ],
        idle_period=20,
        status="active",
    )

    # Assert that all charms that were deployed as part of minimal setup are in correct states.
    assert check_status(ops_test.model.applications[APP_NAME], Status.ACTIVE.value)
    assert (
        ops_test.model.applications[charm_versions.integration_hub.application_name].status
        == "active"
    )
    assert ops_test.model.applications[charm_versions.s3.application_name].status == "active"


@pytest.mark.abort_on_fail
async def test_jdbc_endpoint_with_default_metastore(ops_test: OpsTest, test_pod):
    """Test the JDBC endpoint exposed by the charm."""
    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    kyuubi_unit = ops_test.model.applications[APP_NAME].units[0]
    action = await kyuubi_unit.run_action(
        action_name="get-jdbc-endpoint",
    )
    result = await action.wait()

    jdbc_endpoint = result.results.get("endpoint")
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    logger.info(
        "Testing JDBC endpoint by connecting with beeline and executing a few SQL queries..."
    )

    assert await run_sql_test_against_jdbc_endpoint(ops_test, test_pod)


@pytest.mark.abort_on_fail
async def test_kyuubi_cos_monitoring_setup(ops_test: OpsTest):
    """Setting up COS relations.

    This is important to happen before worker log files start to be generated.
    Only new logs will be picked up by Loki.
    """
    # Prometheus data is being published by the app
    assert await all_prometheus_exporters_data(ops_test, check_field="kyuubi_jvm_uptime")

    # Deploying and relating to grafana-agent
    logger.info("Deploying grafana-agent-k8s charm...")
    await ops_test.model.deploy(COS_AGENT_APP_NAME, num_units=1, series="jammy")

    logger.info("Waiting for test charm to be idle...")
    await ops_test.model.wait_for_idle(apps=[COS_AGENT_APP_NAME], timeout=1000, status="blocked")

    await ops_test.model.integrate(COS_AGENT_APP_NAME, f"{APP_NAME}:metrics-endpoint")
    await ops_test.model.integrate(COS_AGENT_APP_NAME, f"{APP_NAME}:grafana-dashboard")
    await ops_test.model.integrate(COS_AGENT_APP_NAME, f"{APP_NAME}:logging")

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=1000, idle_period=30
    )
    await ops_test.model.wait_for_idle(
        apps=[COS_AGENT_APP_NAME], status="blocked", timeout=1000, idle_period=30
    )

    await ops_test.model.deploy(
        "cos-lite",
        series="jammy",
        trust=True,
    )

    await ops_test.model.wait_for_idle(
        apps=["prometheus", "alertmanager", "loki", "grafana"],
        status="active",
        timeout=2000,
        idle_period=30,
    )
    await ops_test.model.wait_for_idle(
        apps=[COS_AGENT_APP_NAME],
        status="blocked",
        timeout=1000,
        idle_period=30,
    )

    # These two relations --though essential to publishing-- are not set.
    # (May change in the future?)
    try:
        await ops_test.model.integrate(
            f"{COS_AGENT_APP_NAME}:grafana-dashboards-provider", "grafana"
        )
    except juju.errors.JujuAPIError:
        pass

    try:
        await ops_test.model.integrate(f"{COS_AGENT_APP_NAME}:send-remote-write", "prometheus")
    except juju.errors.JujuAPIError:
        pass

    try:
        await ops_test.model.integrate(f"{COS_AGENT_APP_NAME}:logging-consumer", "loki")
    except juju.errors.JujuAPIError:
        pass

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, COS_AGENT_APP_NAME, "prometheus", "alertmanager", "loki", "grafana"],
        status="active",
        timeout=1000,
        idle_period=30,
    )


# @pytest.mark.abort_on_fail
async def test_kyuubi_cos_data_published(ops_test: OpsTest):
    # We should leave time for Prometheus data to be published
    for attempt in Retrying(stop=stop_after_attempt(10), wait=wait_fixed(60), reraise=True):
        with attempt:

            # Data got published to Prometheus
            logger.info("Checking if Prometheus data is being published...")
            cos_address = await get_cos_address(ops_test)
            assert published_prometheus_data(ops_test, cos_address, "kyuubi_jvm_uptime")

            # Alerts got published to Prometheus
            logger.info("Checking if alert rules are published...")
            alerts_data = published_prometheus_alerts(ops_test, cos_address)
            for alert in ["KyuubiBufferPoolCapacityLow", "KyuubiJVMUptime"]:
                assert any(
                    rule["name"] == alert
                    for group in alerts_data["data"]["groups"]
                    for rule in group["rules"]
                )

            # Grafana dashboard got published
            logger.info("Checking the Kyuubi dashboard is available in Grafana...")
            dashboards_info = await published_grafana_dashboards(ops_test)
            assert any(board["title"] == "Kyuubi" for board in dashboards_info)

            # Loki
            logger.info("Checking if Kyuubi server logs are published to Loki...")
            loki_server_logs = await published_loki_logs(
                ops_test, "juju_application", "kyuubi-k8s", 5000
            )
            assert len(loki_server_logs["data"]["result"][0]["values"]) > 0

            # Ideally we should do the check below. However, this requires COS to be started
            # around application startup. Once this is possible, please un-comment the check below
            #
            # assert any(
            #     "Starting org.apache.kyuubi.server.KyuubiServer" in value[1]
            #     for result in loki_server_logs["data"]["result"]
            #     for value in result["values"]
            # )
