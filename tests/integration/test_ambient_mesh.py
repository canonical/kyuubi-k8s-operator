# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import subprocess
from pathlib import Path
from typing import cast

import jubilant
import yaml

from constants import COS_METRICS_PATH, COS_METRICS_PORT
from core.enums import ExposeExternal

from .helpers.auth import LDAP_TEST_PASSWORD, LDAP_TEST_USERNAME, setup_ldap_authentication
from .helpers.cos import (
    assert_grafana_dashboards_published,
    assert_logs_published_in_loki,
    assert_prometheus_alerts_published,
    assert_prometheus_data_exported,
    assert_prometheus_data_published,
    deploy_observability_setup,
)
from .helpers.ha import get_active_kyuubi_servers_list, is_entire_cluster_responding_requests
from .helpers.istio import (
    deploy_istio_mesh_setup,
    has_authorization_policy_from_driver_to_kyuubi,
    has_authorization_policy_to_spark_driver,
    has_authorization_policy_to_spark_executor,
    has_kyuubi_jdbc_authorization_policy,
    has_kyuubi_jdbc_peer_authentication,
)
from .helpers.jdbc import fetch_connection_info, validate_sql_queries_with_kyuubi
from .helpers.juju import get_unit_address
from .helpers.k8s import curl_using_pod, get_pod_ip, get_pod_names, pod_has_labels
from .helpers.kyuubi import deploy_minimal_kyuubi_setup, get_kyuubi_spark_driver_pods
from .types import IntegrationTestsCharms, S3Info, TelemetryAgent

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
AMBIENT_MESH_POD_LABEL_KEY = "istio.io/dataplane-mode"
AMBIENT_MESH_POD_LABEL_VALUE = "ambient"
KYUUBI_JDBC_TEST_USER = "admin"
KYUUBI_JDBC_TEST_USER_PASSWORD = "admin"
SPARK_DRIVER_UI_PORT = 4040


def test_deploy_minimal_kyuubi_setup(
    juju: jubilant.Juju,
    kyuubi_charm: Path,
    charm_versions: IntegrationTestsCharms,
    s3_bucket_and_creds: S3Info,
    workload_namespace: str,
    workload_service_account: str,
) -> None:
    """Deploy the minimal setup for Kyuubi and assert all charms are in active and idle state."""
    deploy_minimal_kyuubi_setup(
        juju=juju,
        kyuubi_charm=kyuubi_charm,
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        integrate_zookeeper=True,
        integrate_data_integrator=True,
        trust=True,
        expose_external=ExposeExternal.LOADBALANCER,
        config={
            "namespace": workload_namespace,
            "service-account": workload_service_account,
        },
    )
    juju.wait(jubilant.all_active, delay=5)


def test_access_from_unmeshed_pod_before_meshing(
    juju: jubilant.Juju,
) -> None:
    """Test the access to the Kyuubi pod from an unmeshed pod before enabling the ambient mesh."""
    pod_ip = get_unit_address(juju, APP_NAME)
    # Using metrics port, since JDBC port would be open for everyone hence not suitable for test
    metrics_url = f"http://{pod_ip}:{COS_METRICS_PORT}{COS_METRICS_PATH}"
    curl_process = curl_using_pod(namespace=juju.model or "default", url=metrics_url)
    assert curl_process.returncode == 0


def test_enable_ambient_mesh_kyuubi(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    workload_namespace: str,
    workload_service_account: str,
) -> None:
    """Enable ambient mesh for the deployed Kyuubi setup."""
    deploy_istio_mesh_setup(
        juju=juju,
        charm_versions=charm_versions,
    )
    for pod_name in get_pod_names(cast(str, juju.model), APP_NAME):
        assert pod_has_labels(
            namespace=cast(str, juju.model),
            pod_name=pod_name,
            labels={AMBIENT_MESH_POD_LABEL_KEY: AMBIENT_MESH_POD_LABEL_VALUE},
        )
    assert has_kyuubi_jdbc_authorization_policy(cast(str, juju.model), APP_NAME)
    assert has_kyuubi_jdbc_peer_authentication(cast(str, juju.model), APP_NAME)


def test_enable_ambient_mesh_integration_hub(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    workload_namespace: str,
    workload_service_account: str,
) -> None:
    logger.info("Adding integration hub into the service mesh...")
    # TODO: Remove this hack, once https://github.com/canonical/service-mesh/issues/813 is fixed
    subprocess.run(
        [
            "kubectl",
            "create",
            "configmap",
            "-n",
            cast(str, juju.model),
            f"juju-service-mesh-{charm_versions.integration_hub.application_name}-labels",
        ],
        check=True,
    )
    juju.integrate(
        f"{charm_versions.integration_hub.application_name}:service-mesh",
        f"{charm_versions.istio_beacon.application_name}:service-mesh",
    )
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=10)
    for pod_name in get_pod_names(
        cast(str, juju.model), charm_versions.integration_hub.application_name
    ):
        assert pod_has_labels(
            namespace=cast(str, juju.model),
            pod_name=pod_name,
            labels={AMBIENT_MESH_POD_LABEL_KEY: AMBIENT_MESH_POD_LABEL_VALUE},
        )
    assert has_authorization_policy_from_driver_to_kyuubi(
        workload_namespace=workload_namespace,
        workload_service_account=workload_service_account,
        kyuubi_namespace=cast(str, juju.model),
        kyuubi_service_account=APP_NAME,
    )
    assert has_authorization_policy_to_spark_driver(
        workload_namespace=workload_namespace,
        workload_service_account=workload_service_account,
        kyuubi_namespace=cast(str, juju.model),
        kyuubi_service_account=APP_NAME,
    )
    assert has_authorization_policy_to_spark_executor(
        workload_namespace=workload_namespace,
        workload_service_account=workload_service_account,
    )


def test_blocked_access_from_unmeshed_pod_after_meshing(
    juju: jubilant.Juju,
) -> None:
    """Test access to the Kyuubi pod from an unmeshed pod is blocked after enabling the ambient mesh."""
    pod_ip = get_unit_address(juju, APP_NAME)
    # Using metrics port, since JDBC port would be open for everyone hence not suitable for test
    pod_url = f"http://{pod_ip}:{COS_METRICS_PORT}{COS_METRICS_PATH}"
    curl_process = curl_using_pod(namespace=juju.model or "default", url=pod_url)
    assert curl_process.returncode != 0


def test_blocked_access_from_meshed_pod_but_no_policy_after_meshing(
    juju: jubilant.Juju,
) -> None:
    """Test access to the Kyuubi pod from a meshed pod without an appropriate policy is blocked."""
    pod_ip = get_unit_address(juju, APP_NAME)
    # Using metrics port, since JDBC port would be open for everyone hence not suitable for test
    pod_url = f"http://{pod_ip}:{COS_METRICS_PORT}{COS_METRICS_PATH}"
    curl_process = curl_using_pod(
        namespace=juju.model or "default",
        url=pod_url,
        labels={"istio.io/dataplane-mode": "ambient"},
    )
    assert curl_process.returncode != 0


def test_sql_queries_with_ambient_mesh(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
) -> None:
    """Test running SQL queries against the deployed Kyuubi setup."""
    jdbc_uri, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    assert validate_sql_queries_with_kyuubi(
        juju=juju, jdbc_uri=jdbc_uri, username=username, password=password
    )


def test_blocked_access_from_unmeshed_pod_to_kyuubi_workload(
    workload_namespace: str,
) -> None:
    driver_pods = get_kyuubi_spark_driver_pods(namespace=workload_namespace)
    assert driver_pods, "No Spark driver pods found in the Kyuubi deployment."
    for driver_pod in driver_pods:
        assert pod_has_labels(
            namespace=workload_namespace,
            pod_name=driver_pod,
            labels={AMBIENT_MESH_POD_LABEL_KEY: AMBIENT_MESH_POD_LABEL_VALUE},
        )
        pod_ip = get_pod_ip(driver_pod, namespace=workload_namespace)
        curl_driver_process = curl_using_pod(
            namespace=workload_namespace, url=f"http://{pod_ip}:{SPARK_DRIVER_UI_PORT}"
        )
        assert curl_driver_process.returncode != 0, (
            f"Access from unmeshed pod to driver pod {driver_pod} should be blocked."
        )


def test_ha_with_ambient_mesh(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    test_pod: str,
) -> None:
    """Test high availability with the ambient mesh enabled for the Kyuubi charm."""
    juju.add_unit(APP_NAME, num_units=2)
    status = juju.wait(jubilant.all_active, delay=10)
    assert len(status.apps[APP_NAME].units) == 3

    active_servers = get_active_kyuubi_servers_list(
        juju=juju, zookeeper_name=charm_versions.zookeeper.app
    )
    assert len(active_servers) == 3

    jdbc_uri, username, password = fetch_connection_info(juju, charm_versions.data_integrator.app)
    assert validate_sql_queries_with_kyuubi(
        juju=juju, jdbc_uri=jdbc_uri, username=username, password=password
    )
    assert is_entire_cluster_responding_requests(
        juju, test_pod, jdbc_endpoint=jdbc_uri, username=username, password=password
    )
    logger.info("Scaling Kyuubi down to 1 pod after the HA test succeeded.")
    juju.remove_unit(APP_NAME, num_units=2)
    status = juju.wait(jubilant.all_active, delay=10)
    assert len(status.apps[APP_NAME].units) == 1


def test_observability_with_ambient_mesh(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
) -> None:
    """Test observability with the ambient mesh enabled for the Kyuubi charm."""
    deploy_observability_setup(
        juju, charm_versions=charm_versions, telemetry_agent=TelemetryAgent.OTEL_COLLECTOR
    )
    logger.info("Putting opentelemetry-collector-k8s into ambient mesh...")
    juju.integrate(
        f"{charm_versions.otel_collector.application_name}:service-mesh",
        f"{charm_versions.istio_beacon.application_name}:service-mesh",
    )
    juju.wait(jubilant.all_active, delay=15)

    assert_prometheus_data_exported(juju, check_field="kyuubi_jvm_uptime")
    assert_prometheus_data_published(juju, check_field="kyuubi_jvm_uptime")
    assert_prometheus_alerts_published(juju)
    assert_grafana_dashboards_published(juju)
    assert_logs_published_in_loki(juju, filter_by_label={"juju_application": "kyuubi-k8s"})


def test_ldap_authentication_with_ambient_mesh(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
) -> None:
    """Test LDAP authentication with the ambient mesh enabled for the Kyuubi charm."""
    logger.info("Removing JDBC authentication relation with PostgreSQL charm")
    juju.remove_relation(f"{APP_NAME}:auth-db", charm_versions.auth_db.application_name)
    juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_blocked(status, APP_NAME),
        delay=5,
    )
    setup_ldap_authentication(juju, charm_versions)
    juju.wait(jubilant.all_active, delay=15)
    jdbc_uri, _, _ = fetch_connection_info(juju, charm_versions.data_integrator.app)
    assert validate_sql_queries_with_kyuubi(
        juju=juju, jdbc_uri=jdbc_uri, username=LDAP_TEST_USERNAME, password=LDAP_TEST_PASSWORD
    )


def test_disable_ambient_mesh_kyuubi(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
) -> None:
    """Test disabling the ambient mesh for the Kyuubi charm."""
    logger.info("Disabling ambient mesh for Kyuubi charm")
    juju.remove_relation(
        f"{APP_NAME}:service-mesh", f"{charm_versions.istio_beacon.application_name}:service-mesh"
    )
    juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status), delay=5
    )
    for pod_name in get_pod_names(cast(str, juju.model), APP_NAME):
        assert not pod_has_labels(
            namespace=cast(str, juju.model),
            pod_name=pod_name,
            labels={AMBIENT_MESH_POD_LABEL_KEY: AMBIENT_MESH_POD_LABEL_VALUE},
        )
    assert not has_kyuubi_jdbc_authorization_policy(cast(str, juju.model), APP_NAME)
    assert not has_kyuubi_jdbc_peer_authentication(cast(str, juju.model), APP_NAME)


def test_disable_ambient_mesh_integration_hub(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    workload_namespace: str,
    workload_service_account: str,
) -> None:
    """Test disabling the ambient mesh for the Integration Hub charm."""
    logger.info("Disabling ambient mesh for Integration Hub charm")
    juju.remove_relation(
        f"{charm_versions.integration_hub.application_name}:service-mesh",
        f"{charm_versions.istio_beacon.application_name}:service-mesh",
    )
    juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status), delay=5
    )
    for pod_name in get_pod_names(
        cast(str, juju.model), charm_versions.integration_hub.application_name
    ):
        assert not pod_has_labels(
            namespace=cast(str, juju.model),
            pod_name=pod_name,
            labels={AMBIENT_MESH_POD_LABEL_KEY: AMBIENT_MESH_POD_LABEL_VALUE},
        )

    assert not has_authorization_policy_from_driver_to_kyuubi(
        workload_namespace=workload_namespace,
        workload_service_account=workload_service_account,
        kyuubi_namespace=cast(str, juju.model),
        kyuubi_service_account=APP_NAME,
    )
    assert not has_authorization_policy_to_spark_driver(
        workload_namespace=workload_namespace,
        workload_service_account=workload_service_account,
        kyuubi_namespace=cast(str, juju.model),
        kyuubi_service_account=APP_NAME,
    )
    assert not has_authorization_policy_to_spark_executor(
        workload_namespace=workload_namespace,
        workload_service_account=workload_service_account,
    )


def test_access_from_unmeshed_pod_after_unmeshing(
    juju: jubilant.Juju,
) -> None:
    """Test accessing the Kyuubi pod from an unmeshed pod after the ambient mesh has been disabled."""
    pod_ip = get_unit_address(juju, APP_NAME)
    # Using metrics port, since JDBC port would be open for everyone hence not suitable for test
    pod_url = f"http://{pod_ip}:{COS_METRICS_PORT}{COS_METRICS_PATH}"
    curl_process = curl_using_pod(namespace=juju.model or "default", url=pod_url)
    assert curl_process.returncode == 0
