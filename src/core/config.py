#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.


"""Structured configuration for the Kyuubi charm."""

import logging

from charms.data_platform_libs.v0.data_models import BaseConfigModel

from .enums import ExposeExternal

logger = logging.getLogger(__name__)


class CharmConfig(BaseConfigModel):
    """Manager for the structured configuration."""

    namespace: str
    service_account: str
    expose_external: ExposeExternal
    loadbalancer_extra_annotations: str
    enable_iceberg: bool
