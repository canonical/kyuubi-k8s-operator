#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import base64
import logging
import os
import subprocess
from pathlib import Path
from platform import machine
from string import Template
from typing import Iterable, cast

import boto3
import boto3.session
import jubilant
import pytest
import yaml
from botocore.client import Config
from dotenv import load_dotenv

from .types import IntegrationTestsCharms, S3Info, TestCharm

load_dotenv()
logger = logging.getLogger(__name__)
logging.getLogger("jubilant.wait").setLevel(logging.WARNING)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
TEST_BUCKET_NAME = "kyuubi-test"
TEST_PATH_NAME = "spark-events/"
TEST_NAMESPACE = "kyuubi-test"
TEST_SERVICE_ACCOUNT = "kyuubi-test"
TEST_POD_SPEC_FILE = "./tests/integration/setup/testpod_spec.yaml.template"


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest, platform: str):
    keep_models = bool(request.config.getoption("--keep-models"))
    model = request.config.getoption("--model")

    if model is None:
        with jubilant.temp_model(keep=keep_models) as juju:
            juju.wait_timeout = 10 * 60
            juju.model_config({"update-status-hook-interval": "60s"})
            juju.model_constraints({"arch": platform})

            yield juju  # run the test

            if request.session.testsfailed:
                log = juju.debug_log(limit=30)
                print(log, end="")
    else:
        juju = jubilant.Juju()
        juju.model = model
        try:
            juju.status()
        except jubilant.CLIError:
            juju.add_model(model)

        juju.wait_timeout = 10 * 60
        juju.model_config({"update-status-hook-interval": "60s"})
        juju.model_constraints({"arch": platform})

        yield juju  # run the test

        if not keep_models:
            juju.destroy_model(model, destroy_storage=True, force=True)


def pytest_addoption(parser):
    parser.addoption(
        "--keep-models",
        action="store_true",
        default=False,
        help="keep temporarily-created models",
    )
    parser.addoption(
        "--model",
        action="store",
        help="Juju model to use; if not provided, a new temporary model "
        "will be created for each test module",
    )


@pytest.fixture(scope="module")
def charm_versions(platform: str) -> IntegrationTestsCharms:
    revisions = {
        "amd64": {
            "s3": 330,
            "metastore": 774,
            "auth": 774,
            "hub": 149,
            "zk": 78,
            "tls": 586,
            "data": 362,
        },
        "arm64": {
            "s3": 332,
            "metastore": 775,
            "auth": 775,
            "hub": 150,
            "zk": 0,  # TODO(zk-arm): Update once we have an arm64 revision
            "tls": 585,
            "data": 359,
        },
    }[platform]

    return IntegrationTestsCharms(
        s3=TestCharm(
            name="s3-integrator",
            channel="1/stable",
            revision=revisions["s3"],
            base="ubuntu@22.04",
            alias="s3",
        ),
        metastore_db=TestCharm(
            name="postgresql-k8s",
            channel="14/stable",
            revision=revisions["metastore"],
            base="ubuntu@22.04",
            alias="metastore",
            trust=True,
        ),
        auth_db=TestCharm(
            name="postgresql-k8s",
            channel="14/stable",
            revision=revisions["auth"],
            base="ubuntu@22.04",
            alias="auth-db",
            trust=True,
        ),
        integration_hub=TestCharm(
            name="spark-integration-hub-k8s",
            channel="3/edge",
            revision=revisions["hub"],
            base="ubuntu@22.04",
            alias="integration-hub",
            trust=True,
        ),
        zookeeper=TestCharm(
            name="zookeeper-k8s",
            channel="3/stable",
            revision=revisions["zk"],
            base="ubuntu@22.04",
            alias="zookeeper",
        ),
        tls=TestCharm(
            name="self-signed-certificates",
            channel="1/stable",
            revision=revisions["tls"],
            base="ubuntu@24.04",
            alias="self-signed-certificates",
        ),
        data_integrator=TestCharm(
            name="data-integrator",
            channel="latest/stable",
            revision=revisions["data"],
            base="ubuntu@24.04",
            alias="data-integrator",
        ),
    )


@pytest.fixture(scope="module")
def s3_bucket_and_creds(request: pytest.FixtureRequest) -> Iterable[S3Info]:
    keep_models = bool(request.config.getoption("--keep-models"))

    access_key = os.environ["S3_ACCESS_KEY"]
    secret_key = os.environ["S3_SECRET_KEY"]
    endpoint_url = os.environ["S3_SERVER_URL"]
    region = os.environ.get("S3_REGION", "us-east-1")
    ca_bundle_path = os.environ.get("S3_CA_BUNDLE_PATH", "")

    session = boto3.session.Session(
        aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region
    )
    s3 = session.resource(
        service_name="s3",
        endpoint_url=endpoint_url,
        verify=ca_bundle_path if ca_bundle_path else False,
        config=Config(
            connect_timeout=60,
            retries={"max_attempts": 4},
            request_checksum_calculation="when_supported",
            response_checksum_validation="when_supported",
        ),
    )
    test_bucket = s3.Bucket(TEST_BUCKET_NAME)

    # Delete test bucket if it exists
    if test_bucket in s3.buckets.all():
        logger.info(f"The bucket {TEST_BUCKET_NAME} already exists. Deleting it...")
        for obj in test_bucket.objects.all():
            # We need to iterate over keys because delete_objects (plural) has mandatory checksum
            obj.delete()
        test_bucket.delete()

    # Create the test bucket
    s3.create_bucket(Bucket=TEST_BUCKET_NAME)
    logger.info(f"Created bucket: {TEST_BUCKET_NAME}")
    test_bucket.put_object(Key=os.path.join(TEST_PATH_NAME, "touch"))
    yield {
        "endpoint": str(endpoint_url),
        "access_key": str(access_key),
        "secret_key": str(secret_key),
        "bucket": TEST_BUCKET_NAME,
        "path": TEST_PATH_NAME,
        "region": region,
        "tls_ca_chain": (
            base64.b64encode(Path(ca_bundle_path).read_bytes()).decode() if ca_bundle_path else ""
        ),
    }

    if not keep_models:
        logger.info("Tearing down test bucket...")
        for obj in test_bucket.objects.all():
            # We need to iterate over keys because delete_objects (plural) has mandatory checksum
            obj.delete()

        test_bucket.delete()


@pytest.fixture(scope="module")
def test_pod(juju: jubilant.Juju) -> Iterable[str]:
    logger.info("Preparing test pod fixture...")

    kyuubi_image = METADATA["resources"]["kyuubi-image"]["upstream-source"]
    namespace = cast(str, juju.model)

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
def platform() -> str:
    """Fixture to provide the platform architecture for testing."""
    platforms = {
        "x86_64": "amd64",
        "aarch64": "arm64",
    }
    return platforms.get(machine(), "amd64")


@pytest.fixture(scope="module")
def kyuubi_charm(platform: str) -> Path:
    """Path to the packed kyuubi charm."""
    if not (path := next(iter(Path.cwd().glob(f"*-{platform}.charm")), None)):
        raise FileNotFoundError("Could not find packed kyuubi charm.")

    return path


@pytest.fixture(scope="module")
def context():
    """A common data store read+writeable by all tests."""
    context = {}
    return context
