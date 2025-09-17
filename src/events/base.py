#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Base utilities exposing common functionalities for all Events classes."""

from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Callable

from ops import EventBase, Object

from core.context import Context
from core.workload.kyuubi import KyuubiWorkload
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KyuubiCharm


class BaseEventHandler(Object, WithLogging):
    """Base class for all Event Handler classes in the Spark Integration Hub."""

    workload: KyuubiWorkload
    charm: KyuubiCharm
    context: Context


def defer_when_not_ready(
    hook: Callable,
) -> Callable[[BaseEventHandler, EventBase], None]:
    """Decorator to defer hook is workload is not ready."""

    @wraps(hook)
    def wrapper_hook(event_handler: BaseEventHandler, event: EventBase):
        """Return none when not ready, proceed with normal hook otherwise."""
        if not event_handler.workload.ready():
            event.defer()
            return None

        return hook(event_handler, event)

    return wrapper_hook


def leader_only(
    hook: Callable,
) -> Callable[[BaseEventHandler, EventBase], None]:
    """Run the hook only on leader unit."""

    @wraps(hook)
    def wrapper_hook(event_handler: BaseEventHandler, event: EventBase):
        """Return none when not leader, proceed with normal hook otherwise."""
        if not event_handler.charm.unit.is_leader():
            return

        return hook(event_handler, event)

    return wrapper_hook
