#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Common classes/functions for K8s implementations."""

import logging
import uuid
from abc import ABC
from collections.abc import Iterator
from contextlib import contextmanager

from ops import Container
from ops.pebble import ExecError
from typing_extensions import override

from common.workload import AbstractWorkload

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
    def read(self, path: str) -> str:
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
            return f.read()

    @override
    def write(self, content: str | bytes, path: str, mode: str = "w") -> None:
        """Writes content to a workload file.

        Args:
            content: string or bytes content to write
            path: the full filepath to write to
            mode: the write mode. Usually "w" for write, or "a" for append. Default "w"
        """
        if isinstance(content, str) and mode == "a" and (current := self.read(path)):
            content = current + "\n" + content
        self.container.push(path, content, make_dirs=True)

    @override
    def delete(self, path: str, recursive: bool = False) -> None:
        """Delete a file or directory from the workload."""
        self.container.remove_path(path, recursive=recursive)

    @override
    def list(self, path: str) -> list[str]:
        """List file paths in a workload directory."""
        return [entry.path for entry in self.container.list_files(path)]

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

    @override
    @contextmanager
    def temporary_file(self, content: str | bytes = "", mode: str = "w") -> Iterator[str]:
        """Provides a temporary file inside the container for use within a context.

        The file is created on entering the context and deleted on exit,
        regardless of whether an exception was raised inside the context.

        Args:
            content: optional initial content to write to the temporary file
            mode: the write mode. Usually "w" for write, or "wb" for bytes. Default "w"

        Yields:
            The full filepath of the temporary file inside the container
        """
        path = f"/tmp/{uuid.uuid4().hex}"
        self.write(content, path, mode=mode)
        try:
            yield path
        finally:
            self.delete(path)
