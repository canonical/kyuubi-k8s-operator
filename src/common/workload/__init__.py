#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Abstract classes for the workload."""

from abc import ABC, abstractmethod


class AbstractWorkload(ABC):
    """Base Abstract class representing the workload."""

    @abstractmethod
    def start(self) -> None:
        """Starts the workload service."""
        ...

    @abstractmethod
    def stop(self) -> None:
        """Stops the workload service."""
        ...

    @abstractmethod
    def exec(
        self, command: str, env: dict[str, str] | None = None, working_dir: str | None = None
    ) -> str:
        """Runs a command on the workload substrate."""
        ...

    @abstractmethod
    def read(self, path: str) -> str:
        """Reads a file from the workload.

        Args:
            path: the full filepath to read from

        Returns:
            String read from the specified path

        Raises:
            FileNotFound if the file does not exist
        """
        ...

    @abstractmethod
    def write(self, content: str | bytes, path: str, mode: str = "w") -> None:
        """Writes content to a workload file.

        Args:
            content: string or bytes content to write
            path: the full filepath to write to
            mode: the write mode. Usually "w" for write, or "a" for append. Default "w"
        """
        ...

    @abstractmethod
    def delete(self, path: str, recursive: bool = False) -> None:
        """Deletes a file or directory from the workload.

        Args:
            path: the full filepath to delete
            recursive: whether to delete directories recursively
        """
        ...

    @abstractmethod
    def list(self, path: str) -> list[str]:
        """Lists file paths in a directory on the workload.

        Args:
            path: the directory path to list
        """
        ...

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check for file existence.

        Args:
            path: the full filepath to be checked for
        """
        ...
