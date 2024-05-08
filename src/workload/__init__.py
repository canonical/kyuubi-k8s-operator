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
    def read(self, path: str) -> list[str]:
        """Reads a file from the workload.

        Args:
            path: the full filepath to read from

        Returns:
            List of string lines from the specified path

        Raises:
            FileNotFound if the file does not exist
        """
        ...

    @abstractmethod
    def write(self, content: str, path: str, mode: str = "w") -> None:
        """Writes content to a workload file.

        Args:
            content: string of content to write
            path: the full filepath to write to
            mode: the write mode. Usually "w" for write, or "a" for append. Default "w"
        """
        ...

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check for file existence.

        Args:
            path: the full filepath to be checked for
        """

    @staticmethod
    def from_env(content: list[str]) -> dict[str, str]:
        """Parse environment file content into a dict structure."""
        map_env = {}
        for var in content:
            key = "".join(var.split("=", maxsplit=1)[0])
            value = "".join(var.split("=", maxsplit=1)[1:])
            if key:
                # only check for keys, as we can have an empty value for a variable
                map_env[key] = value
        return map_env

    @staticmethod
    def to_env(env: dict[str, str]) -> list[str]:
        """Serialize dict into an environment file format."""
        return [f"{key}={value}" for key, value in env.items()]
