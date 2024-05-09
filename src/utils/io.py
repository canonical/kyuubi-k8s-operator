#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Utilities related to file IO."""

import os
from enum import Enum
from io import StringIO
from typing import Union

from ops.model import Container

from core.domain import User

PathLike = Union[str, "os.PathLike[str]"]


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
