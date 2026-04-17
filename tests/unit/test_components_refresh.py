# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import json
from pathlib import Path
from subprocess import check_output

import pytest
import yaml

from constants import GPU_JOB_OCI_IMAGE, JOB_OCI_IMAGE
from events.refresh import is_workload_compatible

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())


def inspect_image(skopeo: str, image: str) -> dict:
    out = check_output([skopeo, "inspect", f"docker://{image}", "--no-creds", "-n"])
    return json.loads(out)


@pytest.mark.slow
def test_images_same_spark_version(skopeo: str) -> None:
    # Given
    charm_workload_image = METADATA["resources"]["kyuubi-image"]["upstream-source"]
    job_image = JOB_OCI_IMAGE
    gpu_job_image = GPU_JOB_OCI_IMAGE

    # we need to strip the tag, as skopeo does not support having both a tag and a digest
    image, _, digest = charm_workload_image.split(":")
    charm_workload_image = f"{image}@sha256:{digest}"
    image, _, digest = job_image.split(":")
    job_image = f"{image}@sha256:{digest}"
    image, _, digest = gpu_job_image.split(":")
    gpu_job_image = f"{image}@sha256:{digest}"

    # When
    workload_version = inspect_image(skopeo, charm_workload_image)["Labels"][
        "org.opencontainers.image.version"
    ]
    job_version = inspect_image(skopeo, job_image)["Labels"]["org.opencontainers.image.version"]
    gpu_job_version = inspect_image(skopeo, gpu_job_image)["Labels"][
        "org.opencontainers.image.version"
    ]

    # Then
    assert workload_version == job_version
    assert workload_version == gpu_job_version


@pytest.mark.parametrize(
    "old, new, expected",
    [
        ("1.9", "1.9", True),
        ("1.9", "1.10", True),
        ("1.9", "1.10.1", True),  # Patch version do not mess with the major.minor logic
        ("1.10", "1.9", False),  # No minor downgrade allowed
        ("1.9", "2.0", False),  # No major bump allowed
    ],
)
def test_is_workload_compatible(old, new, expected):
    # Then
    assert is_workload_compatible(old, new) is expected
