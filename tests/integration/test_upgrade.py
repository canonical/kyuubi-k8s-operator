import logging
import time
from pathlib import Path
from typing import cast

import jubilant
import yaml

from .helpers import (
    deploy_minimal_kyuubi_setup,
    fetch_jdbc_endpoint,
    fetch_password,
    get_active_kyuubi_servers_list,
    run_sql_test_against_jdbc_endpoint,
)
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def test_build_and_deploy(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    s3_bucket_and_creds: S3Info,
    test_pod: str,
) -> None:
    """Test building and deploying the charm without relation with any other charm."""
    deploy_minimal_kyuubi_setup(
        juju=juju,
        kyuubi_charm="kyuubi-k8s",
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=True,
        num_units=3,
        integrate_zookeeper=True,
        deploy_from_charmhub=True,
    )

    # Wait for everything to settle down
    juju.wait(jubilant.all_active, delay=15)

    active_servers = get_active_kyuubi_servers_list(
        juju, zookeeper_name=charm_versions.zookeeper.app
    )
    assert len(active_servers) == 3

    expected_servers = [
        f"kyuubi-k8s-0.kyuubi-k8s-endpoints.{cast(str, juju.model)}.svc.cluster.local",
        f"kyuubi-k8s-1.kyuubi-k8s-endpoints.{cast(str, juju.model)}.svc.cluster.local",
        f"kyuubi-k8s-2.kyuubi-k8s-endpoints.{cast(str, juju.model)}.svc.cluster.local",
    ]
    assert set(active_servers) == set(expected_servers)

    username = "admin"
    password = fetch_password(juju)

    # Run SQL test against the cluster
    jdbc_endpoint = fetch_jdbc_endpoint(juju)
    assert run_sql_test_against_jdbc_endpoint(
        juju, test_pod, jdbc_endpoint, username=username, password=password
    )


def test_kyuubi_upgrades(
    juju: jubilant.Juju, kyuubi_charm: Path, test_pod: str, charm_versions: IntegrationTestsCharms
) -> None:
    """Test the correct upgrade of a Kyuubi cluster."""
    # Retrieve the image to use from metadata.yaml
    image_version = METADATA["resources"]["kyuubi-image"]["upstream-source"]
    logger.info(f"Image version: {image_version}")

    status = juju.status()
    leader_unit = None
    for name, unit in status.apps[APP_NAME].units.items():
        if unit.leader:
            leader_unit = name
    assert leader_unit

    # TODO trigger pre-upgrade checks after the release of the first charm with the upgrade feature available.

    # test upgrade procedure
    logger.info("Upgrading Kyuubi...")

    # start refresh by upgrading to the current version
    juju.refresh(
        APP_NAME,
        path=kyuubi_charm,
        resources={"kyuubi-image": image_version},
    )

    time.sleep(90)

    status = juju.wait(lambda status: jubilant.all_agents_idle(status, APP_NAME), delay=3)

    logger.info("Resume upgrade...")
    leader_unit = None
    for name, unit in status.apps[APP_NAME].units.items():
        if unit.leader:
            leader_unit = name
    assert leader_unit
    try:
        juju.run(leader_unit, "resume-upgrade")
    except Exception:
        pass

    juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=10)

    username = "admin"
    password = fetch_password(juju)

    # test that upgraded Kyuubi cluster works and all units are available
    jdbc_endpoint = fetch_jdbc_endpoint(juju)
    assert run_sql_test_against_jdbc_endpoint(
        juju, test_pod, jdbc_endpoint, username=username, password=password
    )

    active_servers = get_active_kyuubi_servers_list(
        juju, zookeeper_name=charm_versions.zookeeper.application_name
    )
    assert len(active_servers) == 3

    expected_servers = [
        f"kyuubi-k8s-0.kyuubi-k8s-endpoints.{cast(str, juju.model)}.svc.cluster.local",
        f"kyuubi-k8s-1.kyuubi-k8s-endpoints.{cast(str, juju.model)}.svc.cluster.local",
        f"kyuubi-k8s-2.kyuubi-k8s-endpoints.{cast(str, juju.model)}.svc.cluster.local",
    ]
    assert set(active_servers) == set(expected_servers)

    logger.info("End of the tests.")
