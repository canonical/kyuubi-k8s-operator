#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Refresh related event handlers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import charm_refresh

if TYPE_CHECKING:
    from charm import KyuubiCharm


@dataclass(eq=False)
class KyuubiRefresh(charm_refresh.CharmSpecificKubernetes):
    """Implement callbacks and configuration for in-place refreshes."""

    _charm: KyuubiCharm

    @staticmethod
    def run_pre_refresh_checks_after_1_unit_refreshed() -> None:
        """Ignored."""
        pass

    def run_pre_refresh_checks_before_any_units_refreshed(self) -> None:
        """Run compatibility checks."""
        # TODO: Check metastore schema compatibility on refreshed unit
        if not self._charm.metastore_events.metastore_manager.is_metastore_valid():
            raise charm_refresh.PrecheckFailed("Metastore is not valid")

        # TODO: Check image compatibility with regards to spark?
        # We might need to write our own, more permissive, custom logic to handle this use
        # case.

    @classmethod
    def is_compatible(
        cls,
        *,
        old_charm_version: charm_refresh.CharmVersion,
        new_charm_version: charm_refresh.CharmVersion,
        old_workload_version: str,
        new_workload_version: str,
    ) -> bool:
        """Check charm version compatibility."""
        return super().is_compatible(
            old_charm_version=old_charm_version,
            new_charm_version=new_charm_version,
            old_workload_version=old_workload_version,
            new_workload_version=new_workload_version,
        )
