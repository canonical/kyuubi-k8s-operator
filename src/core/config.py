#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.


"""Structured configuration for the Kyuubi charm."""

import logging
import re
from typing import Literal, Optional

from charms.data_platform_libs.v0.data_models import BaseConfigModel
from pydantic import Field, validator

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
    tls_client_private_key: Optional[str] = Field(pattern=SECRET_REGEX, exclude=True)
    profile: Literal["production", "staging", "testing"]
    k8s_node_selectors: str

    @validator("k8s_node_selectors")
    @classmethod
    def k8s_node_selectors_validator(cls, value: str) -> dict[str, str] | None:
        """Check validity of `k8s_node_selectors` field."""
        if not value:
            return None
        res: dict[str, str] = {}
        for selector in value.split(","):
            if selector.count(":") == 1:
                key, val = selector.split(":", 1)
                # check if key and value for selector respect the kubernetes name criteria
                pattern = "^[a-z](?:[a-z0-9\\-]{0,61}[a-z0-9])?$"
                if re.match(pattern, key) and re.match(pattern, val):
                    if key in res.keys():
                        raise ValueError("Duplicate keys in the k8s selector option.")
                    res[key] = val
            else:
                raise ValueError("Malformed k8s_node_selectors options.")
        return res
