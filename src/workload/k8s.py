#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Common classes/functions for K8s implementations."""

import logging
from abc import ABC

from ops import Container
from ops.pebble import ExecError
from typing_extensions import override

from workload import AbstractWorkload

logger = logging.getLogger(__name__)


class K8sWorkload(AbstractWorkload, ABC):
    """Class for providing implementation for IO operations on K8s."""

    container: Container

    def exists(self, path: str) -> bool:
        """Check for file existence.

        Args:
            path: the full filepath to be checked for
        """
        return self.container.exists(path)

    @override
    def read(self, path: str) -> list[str]:
        """Read from a file.

        Args:
            path: the full filepath to be read

        Returns:
            content of the file

        Raises:
            FileNotFound if the file does not exist
        """
        if not self.container.exists(path):
            raise FileNotFoundError

        with self.container.pull(path) as f:
            return f.read().split("\n")

    @override
    def write(self, content: str, path: str, mode: str = "w") -> None:
        """Writes content to a workload file.

        Args:
            content: string of content to write
            path: the full filepath to write to
            mode: the write mode. Usually "w" for write, or "a" for append. Default "w"
        """
        if mode == "a" and (current := self.read(path)):
            content = "\n".join(current + [content])
        self.container.push(path, content, make_dirs=True)

    @override
    def exec(
        self, command: str, env: dict[str, str] | None = None, working_dir: str | None = None
    ) -> str:
        try:
            process = self.container.exec(
                command=command.split(),
                environment=env,
                working_dir=working_dir,
                combine_stderr=True,
            )
            output, _ = process.wait_output()
            return output
        except ExecError as e:
            logger.error(str(e.stderr))
            raise e
