#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import subprocess
from typing import Optional

import boto3
import boto3.session
import pytest
from botocore.client import Config
from pydantic import BaseModel

logger = logging.getLogger(__name__)

TEST_BUCKET_NAME = "kyuubi-test"
TEST_NAMESPACE = "kyuubi-test"
TEST_SERVICE_ACCOUNT = "kyuubi-test"

class MockCharm(BaseModel):
    """An mock abstraction of a charm to be deployed.

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
    num_units: int = 1
    alias: Optional[str] = None

    @property
    def application_name(self) -> str:
        return self.alias or self.name

    def deploy_dict(self):
        return {
            "entity_url": self.name,
            "channel": self.channel,
            "series": self.series,
            "num_units": self.num_units,
            "application_name": self.application_name,
        }


class IntegrationTestsCharms(BaseModel):
    s3: MockCharm


@pytest.fixture
def charm_versions() -> IntegrationTestsCharms:
    return IntegrationTestsCharms(
        s3=MockCharm(
            **{"name": "s3-integrator", "channel": "edge", "series": "jammy", "alias": "s3"}
        ),
    )


@pytest.fixture
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

    return {
        "endpoint": endpoint_url,
        "access_key": access_key,
        "secret_key": secret_key,
        "bucket": TEST_BUCKET_NAME,
    }


@pytest.fixture
def service_account():
    logger.info("Preparing service account fixture...")
    assert subprocess.run(
        [
            "./tests/integration/setup/setup_service_account.sh",
            TEST_NAMESPACE,
            TEST_SERVICE_ACCOUNT
        ],
        check=True,
    ).returncode == 0
    return TEST_NAMESPACE, TEST_SERVICE_ACCOUNT