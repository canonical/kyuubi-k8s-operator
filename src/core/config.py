#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.


"""Structured configuration for the Kyuubi charm."""

import logging
import re
from typing import Literal

from charms.data_platform_libs.v0.data_models import BaseConfigModel
from pydantic import Field, NonNegativeInt, PositiveInt, validator

from .enums import ExposeExternal

logger = logging.getLogger(__name__)

SECRET_REGEX = re.compile("secret:[a-z0-9]{20}")


class CharmConfig(BaseConfigModel):
    """Manager for the structured configuration."""

    driver_pod_template: str
    enable_dynamic_allocation: bool
    executor_cores: PositiveInt | None
    executor_memory: PositiveInt | None
    executor_pod_template: str
    expose_external: ExposeExternal
    gpu_enable: bool
    gpu_engine_executors_limit: PositiveInt | Literal[-1]
    gpu_pinned_memory: NonNegativeInt
    iceberg_catalog_name: str
    k8s_node_selectors: dict[str, str] | None
    loadbalancer_extra_annotations: str
    namespace: str
    profile: Literal["production", "staging", "testing"]
    service_account: str
    system_users: str | None = Field(pattern=SECRET_REGEX, exclude=True)
    tls_client_private_key: str | None = Field(pattern=SECRET_REGEX, exclude=True)

    @validator("k8s_node_selectors", pre=True)
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
