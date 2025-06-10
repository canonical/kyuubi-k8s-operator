#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

"""Refresh related event handlers."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import charm_refresh

if TYPE_CHECKING:
    from charm import KyuubiCharm

logger = logging.getLogger(__name__)


@dataclass(eq=False)
class KyuubiRefresh(charm_refresh.CharmSpecificKubernetes):
    """Implement callbacks and configuration for in-place refreshes."""

    _charm: KyuubiCharm

    def run_pre_refresh_checks_after_1_unit_refreshed(self) -> None:
        """Checks to run before and during refresh."""
        if (planned_units := self._charm.model.app.planned_units()) != len(
            self._charm.context.app_units
        ):
            # We don't need to check for the peer relation since `charm_refresh.PeerRelationNotReady`
            # would've been raised otherwise
            raise charm_refresh.PrecheckFailed(
                "Cluster is unstable; unit addition/removal ongoing"
            )

        if planned_units == 1:
            logger.warning(
                "Refreshing a single unit deployment is not recommended, "
                "as compatibility check will not be run."
            )

        if not self._charm.context.metastore_db:
            logging.warning(
                "Application is not related to an external metastore. "
                "Refreshing will lead to local data loss."
            )
        elif not self._charm.metastore_events.metastore_manager.is_metastore_valid():
            raise charm_refresh.PrecheckFailed("Metastore is invalid")

    @classmethod
    def is_compatible(
        cls,
        *,
        old_charm_version: charm_refresh.CharmVersion,
        new_charm_version: charm_refresh.CharmVersion,
        old_workload_version: str,
        new_workload_version: str,
    ) -> bool:
        """Check compatibility.

        On top of the default compatibility check, we need:
        - same MAJOR.MINOR for spark, enforced at the 'track' level
        - same MAJOR for kyuubi, greater or equal MINOR, enforced by the 'workload' key
          in refresh_versions.toml
        """
        if not old_charm_version.track == new_charm_version.track:
            logger.error(
                "Upgrading to a different track is not supported. "
                f"Got {old_charm_version.track} to {new_charm_version.track}"
            )
            return False

        if not super().is_compatible(
            old_charm_version=old_charm_version,
            new_charm_version=new_charm_version,
            old_workload_version=old_workload_version,
            new_workload_version=new_workload_version,
        ):
            # The call above will log the reason
            return False

        return is_workload_compatible(old_workload_version, new_workload_version)


def is_workload_compatible(old_workload_version: str, new_workload_version: str) -> bool:
    """Define Kyuubi workload compatibility strategy.

    Patch version are ignored.
    """
    try:
        old_major, old_minor, *_ = (
            int(component) for component in old_workload_version.split(".")
        )
        new_major, new_minor, *_ = (
            int(component) for component in new_workload_version.split(".")
        )
    except ValueError:
        # Not enough values to unpack or cannot convert
        logger.error(
            "Unable to parse workload versions."
            f"Got {old_workload_version} to {new_workload_version}"
        )
        return False

    if old_major != new_major:
        logger.error(
            "Upgrading to a different major workload is not supported. "
            f"Got {old_major} to {new_major}"
        )
        return False

    if not new_minor >= old_minor:
        logger.error(
            "Upgrading to a previous minor workload is not supported. "
            f"Got {old_major}.{old_minor} to {new_major}.{new_minor}"
        )
        return False

    return True
