#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.


"""Structured configuration for the Kyuubi charm."""

import logging
import re
from typing import Optional

from charms.data_platform_libs.v0.data_models import BaseConfigModel
from pydantic import Field
from pydantic import validator

from .enums import ExposeExternal

logger = logging.getLogger(__name__)

SECRET_REGEX = re.compile("secret:[a-z0-9]{20}")


class CharmConfig(BaseConfigModel):
    """Manager for the structured configuration."""

    namespace: str
    service_account: str
    expose_external: ExposeExternal
    loadbalancer_extra_annotations: str
    enable_dynamic_allocation: bool
    iceberg_catalog_name: str
    system_users: Optional[str] = Field(pattern=SECRET_REGEX, exclude=True)
    profile: str

    @validator("profile")
    @classmethod
    def profile_values(cls, value: str) -> str | None:
        """Check profile config option is one of `testing`, `staging` or `production`."""
        if value not in ["testing", "staging", "production"]:
            raise ValueError("Value not one of 'testing', 'staging' or 'production'")
        return value
