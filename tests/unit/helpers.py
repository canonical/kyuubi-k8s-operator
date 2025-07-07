# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

from pathlib import Path

SPARK_PROPERTIES = "/etc/spark8t/conf/spark-defaults.conf"


def parse_spark_properties(tmp_path: Path) -> dict[str, str]:
    """Parse and return spark properties from the conf file in the container."""
    file_path = tmp_path / Path(SPARK_PROPERTIES).relative_to("/etc")
    with file_path.open("r") as fid:
        return dict(
            row.rsplit("=", maxsplit=1) for line in fid.readlines() if (row := line.strip())
        )
