# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import json
from pathlib import Path
from subprocess import check_output

import yaml

from constants import KYUUBI_OCI_IMAGE

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())


def inspect_image(skopeo: str, image: str) -> dict:
    out = check_output([skopeo, "inspect", f"docker://{image}", "--no-creds"])
    return json.loads(out)


def test_images_same_spark_version(skopeo: str) -> None:
    # Given
    charm_workload_image = METADATA["resources"]["kyuubi-image"]["upstream-source"]
    job_image = KYUUBI_OCI_IMAGE

    # When
    workload_version = inspect_image(skopeo, charm_workload_image)["Labels"][
        "org.opencontainers.image.version"
    ]
    job_version = inspect_image(skopeo, job_image)["Labels"]["org.opencontainers.image.version"]

    # Then
    assert workload_version == job_version
