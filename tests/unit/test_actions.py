#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import dataclasses
import json
import logging
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from ops.testing import ActionFailed, Container, Context, Relation, State

from charm import KyuubiCharm
from constants import KYUUBI_CONTAINER_NAME
from core.domain import Status

logger = logging.getLogger(__name__)


CONFIG = yaml.safe_load(Path("./config.yaml").read_text())
ACTIONS = yaml.safe_load(Path("./actions.yaml").read_text())
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())


@pytest.fixture()
def charm_configuration():
    """Enable direct mutation on configuration dict."""
    return json.loads(json.dumps(CONFIG))


@pytest.fixture()
def base_state():
    return State(leader=True, containers=[Container(name=KYUUBI_CONTAINER_NAME, can_connect=True)])


@pytest.fixture()
def ctx() -> Context:
    ctx = Context(
        KyuubiCharm,
        meta=METADATA,
        config=CONFIG,
        actions=ACTIONS,
    )
    return ctx


@pytest.mark.parametrize(
    ["action"], [("get-password",), ("set-password",), ("get-jdbc-endpoint",)]
)
def test_action_not_leader(action: str, ctx: Context, base_state: State) -> None:
    # Given
    state_in = dataclasses.replace(base_state, leader=False)

    # When
    # Then
    with pytest.raises(ActionFailed) as exc_info:
        _ = ctx.run(ctx.on.action(action), state_in)

    assert exc_info.value.message == "Action must be ran on the application leader"


@pytest.mark.parametrize(["action"], [("get-password",), ("set-password",)])
def test_password_action_not_related(ctx: Context, base_state: State, action: str) -> None:
    # Given
    state_in = base_state

    # When
    # Then
    with pytest.raises(ActionFailed) as exc_info:
        _ = ctx.run(ctx.on.action(action), state_in)

    assert "The action can only be run when authentication is enabled" in exc_info.value.message


@pytest.mark.parametrize(
    ["action"], [("get-password",), ("set-password",), ("get-jdbc-endpoint",)]
)
def test_action_workload_not_ready(ctx: Context, action: str) -> None:
    # Given
    relation_db = Relation(
        "auth-db",
        "postgresql_client",
        remote_app_data={
            "endpoints": "127.0.0.1:5432",
            "username": "speakfriend",
            "password": "mellon",
            "database": "lotr",
        },
    )
    state_in = State(
        leader=True,
        containers=[Container(name=KYUUBI_CONTAINER_NAME, can_connect=False)],
        relations=[relation_db],
    )

    # When
    # Then
    with pytest.raises(ActionFailed) as exc_info:
        _ = ctx.run(ctx.on.action(action), state_in)

    assert exc_info.value.message == "The action failed because the workload is not ready yet."


@pytest.mark.parametrize(
    ["action"], [("get-password",), ("set-password",), ("get-jdbc-endpoint",)]
)
def test_action_workload_not_active(ctx: Context, base_state: State, action: str) -> None:
    # Given
    relation_db = Relation(
        "auth-db",
        "postgresql_client",
        remote_app_data={
            "endpoints": "127.0.0.1:5432",
            "username": "speakfriend",
            "password": "mellon",
            "database": "lotr",
        },
    )
    state_in = dataclasses.replace(base_state, relations=[relation_db])

    # When
    # Then
    with (
        patch(
            "events.base.BaseEventHandler.get_app_status",
            return_value=Status.MISSING_OBJECT_STORAGE_BACKEND,
        ),
        pytest.raises(ActionFailed) as exc_info,
    ):
        _ = ctx.run(ctx.on.action(action), state_in)

    assert exc_info.value.message == "The action failed because the charm is not in active state."
