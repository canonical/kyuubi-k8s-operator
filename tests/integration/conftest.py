#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import subprocess
from pathlib import Path
from string import Template
from typing import Optional

import boto3
import boto3.session
import pytest
import yaml
from botocore.client import Config
from pydantic import BaseModel

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
TEST_BUCKET_NAME = "kyuubi-test"
TEST_PATH_NAME = "spark-events/"
TEST_NAMESPACE = "kyuubi-test"
TEST_SERVICE_ACCOUNT = "kyuubi-test"
TEST_POD_SPEC_FILE = "./tests/integration/setup/testpod_spec.yaml.template"


class TestCharm(BaseModel):
    """An abstraction of metadata of a charm to be deployed.

    Attrs:
        name: str, representing the charm to be deployed
        channel: str, representing the channel to be used
        series: str, representing the series of the system for the container where the charm
            is deployed to
        num_units: int, number of units for the deployment
        alias: str (Optional), alias to be used for the charm
    """

    name: str
    channel: str
    series: str
    revision: int
    num_units: int = 1
    alias: Optional[str] = None
    trust: Optional[bool] = False

    @property
    def application_name(self) -> str:
        return self.alias or self.name

    def deploy_dict(self):
        return {
            "entity_url": self.name,
            "channel": self.channel,
            "series": self.series,
            "revision": self.revision,
            "num_units": self.num_units,
            "application_name": self.application_name,
            "trust": self.trust,
        }


class IntegrationTestsCharms(BaseModel):
    s3: TestCharm
    postgres: TestCharm
    auth_db: TestCharm
    integration_hub: TestCharm
    zookeeper: TestCharm
    tls: TestCharm


@pytest.fixture(scope="module")
def charm_versions() -> IntegrationTestsCharms:
    return IntegrationTestsCharms(
        s3=TestCharm(
            **{
                "name": "s3-integrator",
                "channel": "edge",
                "revision": 41,
                "series": "jammy",
                "alias": "s3",
            }
        ),
        postgres=TestCharm(
            **{
                "name": "postgresql-k8s",
                "channel": "14/stable",
                "revision": 281,
                "series": "jammy",
                "alias": "postgresql",
                "trust": True,
            }
        ),
        auth_db=TestCharm(
            **{
                "name": "postgresql-k8s",
                "channel": "14/stable",
                "revision": 281,
                "series": "jammy",
                "alias": "auth-db",
                "trust": True,
            }
        ),
        integration_hub=TestCharm(
            **{
                "name": "spark-integration-hub-k8s",
                "channel": "latest/edge",
                "revision": 43,
                "series": "jammy",
                "alias": "integration-hub",
                "trust": True,
            }
        ),
        zookeeper=TestCharm(
            **{
                "name": "zookeeper-k8s",
                "channel": "3/edge",
                "revision": 70,
                "series": "jammy",
                "alias": "zookeeper",
                "num_units": 1,
            }
        ),
        tls=TestCharm(
            **{
                "name": "self-signed-certificates",
                "channel": "edge",
                "revision": 163,  # FIXME (certs): Unpin the revision once the charm is fixed
                "series": "jammy",
                "alias": "self-signed-certificates",
                "num_units": 1,
            }
        ),
    )


@pytest.fixture(scope="module")
def s3_bucket_and_creds():
    logger.info("Fetching S3 credentials from minio.....")

    fetch_s3_output = (
        subprocess.check_output(
            "./tests/integration/setup/fetch_s3_credentials.sh | tail -n 3",
            shell=True,
            stderr=None,
        )
        .decode("utf-8")
        .strip()
    )

    logger.info(f"fetch_s3_credentials output:\n{fetch_s3_output}")

    endpoint_url, access_key, secret_key = fetch_s3_output.strip().splitlines()

    session = boto3.session.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3 = session.resource(
        service_name="s3",
        endpoint_url=endpoint_url,
        verify=False,
        config=Config(connect_timeout=60, retries={"max_attempts": 4}),
    )
    test_bucket = s3.Bucket(TEST_BUCKET_NAME)

    # Delete test bucket if it exists
    if test_bucket in s3.buckets.all():
        logger.info(f"The bucket {TEST_BUCKET_NAME} already exists. Deleting it...")
        test_bucket.objects.all().delete()
        test_bucket.delete()

    # Create the test bucket
    s3.create_bucket(Bucket=TEST_BUCKET_NAME)
    logger.info(f"Created bucket: {TEST_BUCKET_NAME}")
    test_bucket.put_object(Key=TEST_PATH_NAME)
    yield {
        "endpoint": endpoint_url,
        "access_key": access_key,
        "secret_key": secret_key,
        "bucket": TEST_BUCKET_NAME,
        "path": TEST_PATH_NAME,
    }

    logger.info("Tearing down test bucket...")
    test_bucket.objects.all().delete()
    test_bucket.delete()


@pytest.fixture(scope="module")
def test_pod(ops_test):
    logger.info("Preparing test pod fixture...")

    kyuubi_image = METADATA["resources"]["kyuubi-image"]["upstream-source"]
    namespace = ops_test.model_name

    with open(TEST_POD_SPEC_FILE) as tf:
        template = Template(tf.read())
        pod_spec = template.substitute(kyuubi_image=kyuubi_image, namespace=namespace)

    # Create test pod by applying pod spec
    apply_result = subprocess.run(
        ["kubectl", "apply", "-f", "-"], check=True, input=pod_spec.encode()
    )
    assert apply_result.returncode == 0

    pod_name = yaml.safe_load(pod_spec)["metadata"]["name"]

    # Wait until the pod is in ready state
    wait_result = subprocess.run(
        [
            "kubectl",
            "wait",
            "--for",
            "condition=Ready",
            f"pod/{pod_name}",
            "-n",
            namespace,
            "--timeout",
            "60s",
        ]
    )
    assert wait_result.returncode == 0

    # Yield the name of created pod
    yield pod_name

    # Cleanup by deleting the pod that was created
    logger.info("Deleting test pod fixture...")
    delete_result = subprocess.run(
        ["kubectl", "delete", "pod", "-n", namespace, pod_name], check=True
    )
    assert delete_result.returncode == 0


@pytest.fixture(scope="module")
def kyuubi_charm() -> Path:
    """Path to the packed integration hub charm."""
    if not (path := next(iter(Path.cwd().glob("*.charm")), None)):
        raise FileNotFoundError("Could not find packed kyuubi charm.")

    return path


@pytest.fixture(scope="module")
def test_charm() -> Path:
    if not (
        path := next(iter((Path.cwd() / "tests/integration/app-charm").glob("*.charm")), None)
    ):
        raise FileNotFoundError("Could not find packed test charm.")

    return path
