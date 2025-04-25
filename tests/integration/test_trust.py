#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import pytest
import yaml

from core.domain import Status

from .helpers import deploy_minimal_kyuubi_setup
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


@pytest.mark.abort_on_fail
def test_build_and_deploy(
    juju: jubilant.Juju,
    kyuubi_charm: Path,
    charm_versions: IntegrationTestsCharms,
    s3_bucket_and_creds: S3Info,
) -> None:
    deploy_minimal_kyuubi_setup(
        juju=juju,
        kyuubi_charm=kyuubi_charm,
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=False,
    )

    # Assert that all charms that were deployed as part of minimal setup are in correct states.
    status = juju.wait(
        lambda status: jubilant.all_active(
            status, charm_versions.integration_hub.name, charm_versions.s3.name
        ),
        timeout=120,
        delay=3,
    )

    assert (
        status.apps[APP_NAME].app_status.message
        == Status.INSUFFICIENT_CLUSTER_PERMISSIONS.value.message
    )


def test_provide_clusterwide_trust_permissions(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    # Add cluster-wide trust permission on the application
    juju.trust(APP_NAME, scope="cluster")

    juju.wait(
        lambda status: jubilant.all_active(
            status, APP_NAME, charm_versions.s3.name, charm_versions.integration_hub.name
        ),
        delay=5,
    )
