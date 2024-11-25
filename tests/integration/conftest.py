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
from juju.application import Application
from juju.unit import Unit
from ops import StatusBase
from pydantic import BaseModel

from core.domain import Status

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
    integration_hub: TestCharm
    zookeeper: TestCharm


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
        integration_hub=TestCharm(
            **{
                "name": "spark-integration-hub-k8s",
                "channel": "latest/edge",
                "revision": 19,
                "series": "jammy",
                "alias": "integration-hub",
                "trust": True,
            }
        ),
        zookeeper=TestCharm(
            **{
                "name": "zookeeper-k8s",
                "channel": "3/edge",
                "revision": 59,
                "series": "jammy",
                "alias": "zookeeper",
                "num_units": 3,
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
        config=Config(connect_timeout=60, retries={"max_attempts": 0}),
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
async def kyuubi_charm(ops_test):
    logger.info("Building charm...")
    charm = await ops_test.build_charm(".")
    return charm


def check_status(entity: Application | Unit, status: StatusBase):
    if isinstance(entity, Application):
        return entity.status == status.name and entity.status_message == status.message
    elif isinstance(entity, Unit):
        return (
            entity.workload_status == status.name
            and entity.workload_status_message == status.message
        )
    else:
        raise ValueError(f"entity type {type(entity)} is not allowed")


@pytest.fixture(scope="module")
async def minimal_kyuubi_setup(ops_test, kyuubi_charm, charm_versions, s3_bucket_and_creds):
    # Deploy the charm and wait for waiting status
    logger.info("Deploying kyuubi-k8s charm...")
    await ops_test.model.deploy(
        kyuubi_charm,
        application_name=APP_NAME,
        num_units=1,
        channel="edge",
        series="jammy",
        trust=True,
    )

    logger.info("Waiting for kyuubi-k8s app to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        timeout=1000,
    )
    logger.info(f"State of kyuubi-k8s app: {ops_test.model.applications[APP_NAME].status}")

    logger.info("Setting configuration for kyuubi-k8s charm...")
    namespace = ops_test.model.name
    username = "kyuubi-spark-engine"
    await ops_test.model.applications[APP_NAME].set_config(
        {"namespace": namespace, "service-account": username}
    )

    logger.info("Waiting for kyuubi-k8s app to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="blocked",
        timeout=1000,
    )

    # Assert that the charm is in blocked state, waiting for Integration Hub relation
    assert check_status(
        ops_test.model.applications[APP_NAME], Status.MISSING_INTEGRATION_HUB.value
    )

    # Deploy the charm and wait for waiting status
    logger.info("Deploying s3-integrator charm...")
    await ops_test.model.deploy(**charm_versions.s3.deploy_dict())

    logger.info("Waiting for s3-integrator app to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.s3.application_name], timeout=1000
    )

    # Receive S3 params from fixture
    endpoint_url = s3_bucket_and_creds["endpoint"]
    access_key = s3_bucket_and_creds["access_key"]
    secret_key = s3_bucket_and_creds["secret_key"]
    bucket_name = s3_bucket_and_creds["bucket"]

    logger.info("Setting up s3 credentials in s3-integrator charm")
    s3_integrator_unit = ops_test.model.applications[charm_versions.s3.application_name].units[0]
    action = await s3_integrator_unit.run_action(
        action_name="sync-s3-credentials", **{"access-key": access_key, "secret-key": secret_key}
    )
    await action.wait()

    logger.info("Setting configuration for s3-integrator charm...")
    await ops_test.model.applications[charm_versions.s3.application_name].set_config(
        {
            "bucket": bucket_name,
            "path": "testpath",
            "endpoint": endpoint_url,
        }
    )

    logger.info("Waiting for s3-integrator app to be idle and active...")
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[charm_versions.s3.application_name], status="active"
        )

    # Deploy the charm and wait for waiting status
    logger.info("Deploying integration-hub charm...")
    await ops_test.model.deploy(**charm_versions.integration_hub.deploy_dict())

    logger.info("Waiting for integration_hub and s3-integrator app to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[charm_versions.integration_hub.application_name, charm_versions.s3.application_name],
        timeout=1000,
        status="active",
    )

    logger.info("Integrating integration-hub charm with s3-integrator charm...")
    await ops_test.model.integrate(
        charm_versions.s3.application_name, charm_versions.integration_hub.application_name
    )

    logger.info("Waiting for kyuubi-k8s, s3-integrator and integration-hub charms to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[
            charm_versions.integration_hub.application_name,
            charm_versions.s3.application_name,
            APP_NAME,
        ],
        timeout=1000,
    )

    # Add configuration key
    unit = ops_test.model.applications[charm_versions.integration_hub.application_name].units[0]
    action = await unit.run_action(
        action_name="add-config", conf="spark.kubernetes.executor.request.cores=0.1"
    )
    _ = await action.wait()

    logger.info("Integrating kyuubi charm with integration-hub charm...")
    await ops_test.model.integrate(charm_versions.integration_hub.application_name, APP_NAME)

    logger.info(
        "Waiting for s3-integrator, integration_hub and kyuubi charms to be idle and active..."
    )
    await ops_test.model.wait_for_idle(
        apps=[
            APP_NAME,
            charm_versions.integration_hub.application_name,
            charm_versions.s3.application_name,
        ],
        timeout=1000,
        status="active",
        idle_period=20,
    )

    # Assert that all kyuubi-k8s, integration-hub and s3-integrator charms are in active state
    assert check_status(ops_test.model.applications[APP_NAME], Status.ACTIVE.value)
    assert (
        ops_test.model.applications[charm_versions.integration_hub.application_name].status
        == "active"
    )
    assert ops_test.model.applications[charm_versions.s3.application_name].status == "active"
