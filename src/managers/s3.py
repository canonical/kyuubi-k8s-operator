#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 connection manager."""

from functools import cached_property

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from core.domain import S3ConnectionInfo
from utils.logging import WithLogging


class S3Manager(WithLogging):
    """Class representing credentials and endpoints to connect to S3."""

    def __init__(self, s3_info: S3ConnectionInfo):
        self.s3_info = s3_info

    @cached_property
    def session(self):
        """Return the S3 session to be used when connecting to S3."""
        return boto3.session.Session(
            aws_access_key_id=self.s3_info.access_key,
            aws_secret_access_key=self.s3_info.secret_key,
        )

    def verify(self) -> bool:
        """Verify S3 credentials."""
        s3 = self.session.client(
            "s3", endpoint_url=self.s3_info.endpoint or "https://s3.amazonaws.com"
        )

        try:
            s3.list_buckets()
        except (ClientError, NoCredentialsError):
            self.logger.error("Invalid S3 credentials...")
            return False

        return True
