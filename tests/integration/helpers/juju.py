# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import jubilant


def get_leader_unit(juju: jubilant.Juju, app: str) -> str:
    """Get application leader unit."""
    status = juju.status()
    leader_unit = None
    for name, unit in status.apps[app].units.items():
        if unit.leader:
            leader_unit = name
    assert leader_unit, f"No leader unit found for {app}"
    return leader_unit


def get_unit_address(
    juju: jubilant.Juju,
    app_name: str,
    unit_number: int = 0,
) -> str:
    """Retrieve the IP address of a specific unit of an application."""
    status = juju.status()
    address = status.apps[app_name].units[f"{app_name}/{unit_number}"].address
    return address
