#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 connection utility classes and methods."""

from dataclasses import dataclass
from functools import cached_property

import boto3
from botocore.exceptions import ClientError

from utils import WithLogging


@dataclass
class S3ConnectionInfo(WithLogging):
    """Class representing credentials and endpoints to connect to S3."""

    endpoint: str | None
    access_key: str
    secret_key: str
    path: str
    bucket: str

    @cached_property
    def session(self):
        """Return the S3 session to be used when connecting to S3."""
        return boto3.session.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    def verify(self) -> bool:
        """Verify S3 credentials."""
        s3 = self.session.client("s3", endpoint_url=self.endpoint or "https://s3.amazonaws.com")

        try:
            s3.list_buckets()
            self.logger.info(f"s3 buckets: {s3.list_buckets()}")
        except ClientError:
            self.logger.error("Invalid S3 credentials...")
            return False

        return True
