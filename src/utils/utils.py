#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Miscellaneous utilities."""

import os
from enum import Enum
from io import StringIO
from logging import Logger, getLogger
from typing import Any, Callable, Literal, TypedDict, Union

from ops.model import Container

from models import User

PathLike = Union[str, "os.PathLike[str]"]

LevelTypes = Literal[
    "CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET", 50, 40, 30, 20, 10, 0
]
StrLevelTypes = Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"]


class LevelsDict(TypedDict):
    """Log Levels."""

    CRITICAL: Literal[50]
    ERROR: Literal[40]
    WARNING: Literal[30]
    INFO: Literal[20]
    DEBUG: Literal[10]
    NOTSET: Literal[0]


DEFAULT_LOG_LEVEL: StrLevelTypes = "INFO"

levels: LevelsDict = {
    "CRITICAL": 50,
    "ERROR": 40,
    "WARNING": 30,
    "INFO": 20,
    "DEBUG": 10,
    "NOTSET": 0,
}


class WithLogging:
    """Base class to be used for providing a logger embedded in the class."""

    @property
    def logger(self) -> Logger:
        """Create logger.

        :return: default logger.
        """
        name_logger = str(self.__class__).replace("<class '", "").replace("'>", "")
        return getLogger(name_logger)

    def log_result(
        self, msg: Union[Callable[..., str], str], level: StrLevelTypes = "INFO"
    ) -> Callable[..., Any]:
        """Return a decorator to allow logging of inputs/outputs.

        :param msg: message to log
        :param level: logging level
        :return: wrapped method.
        """

        def wrap(x: Any) -> Any:
            if isinstance(msg, str):
                self.logger.log(levels[level], msg)
            else:
                self.logger.log(levels[level], msg(x))
            return x

        return wrap


class IOMode(str, Enum):
    """Class representing the modes to open file resources."""

    READ = "r"
    WRITE = "w"


class ContainerFile(StringIO):
    """Class representing a file in the workload container to be read/written.

    The operations will be mediated by Pebble, but this should be abstracted away such
    that the same API can also be used for files in local file systems. This allows to
    create some context where handling read/write independently from the substrate:

    ```python
    file = ContainerFile(container, user, IOMode.READ)
    # or open("local-file", IOMode.READ)

    with file as fid:
        fid.read()
    ```
    """

    def __init__(self, container: Container, user: User, path: str, mode: IOMode):
        super().__init__()
        self.container = container
        self.user = user
        self.path = path
        self._mode = mode

    def exists(self):
        """Check whether the file exists."""
        return self.container.exists(self.path)

    def open(self):
        """Execute business logic on context creation."""
        if self._mode is IOMode.READ:
            self.write(self.container.pull(self.path).read().decode("utf-8"))

    def close(self):
        """Execute business logic on context destruction."""
        if self._mode is IOMode.WRITE:
            self.container.push(
                self.path,
                self.getvalue(),
                user=self.user.name,
                group=self.user.group,
                make_dirs=True,
                permissions=0o640,
            )
