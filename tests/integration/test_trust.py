#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import yaml

from .helpers import deploy_minimal_kyuubi_setup
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


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
        trust=True,
    )

    # Assert that all charms that were deployed as part of minimal setup are in correct states.
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            APP_NAME,
            charm_versions.integration_hub.application_name,
            charm_versions.s3.application_name,
        ),
        delay=5,
    )


def test_remove_clusterwide_trust_permissions(juju: jubilant.Juju) -> None:
    juju.trust(APP_NAME, scope="cluster", remove=True)

    status = juju.wait(
        lambda status: jubilant.any_blocked(
            status,
            APP_NAME,
        ),
    )
    assert (
        status.apps[APP_NAME].app_status.message
        == "Run `juju trust kyuubi-k8s --scope=cluster`. Needed for in-place refreshes"
    )
