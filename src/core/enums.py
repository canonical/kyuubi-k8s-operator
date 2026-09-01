"""Module that contains enums used in the charm."""

from enum import Enum


class ExposeExternal(str, Enum):
    """Enum for the `expose-external` field."""

    FALSE = "false"
    NODEPORT = "nodeport"
    LOADBALANCER = "loadbalancer"


class CleanupDriverPods(str, Enum):
    """Enum for the `cleanup-terminated-driver-pods` field."""

    NONE = "none"
    COMPLETED = "completed"
    ALL = "all"
