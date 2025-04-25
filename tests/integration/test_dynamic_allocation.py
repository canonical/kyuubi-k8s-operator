#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
import time
from pathlib import Path
from typing import cast

import jubilant
import yaml
from spark_test.core.kyuubi import KyuubiClient
from spark_test.utils import get_spark_executors

from .helpers import (
    deploy_minimal_kyuubi_setup,
)
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def test_deploy_kyuubi_setup(
    juju: jubilant.Juju,
    kyuubi_charm: Path,
    charm_versions: IntegrationTestsCharms,
    s3_bucket_and_creds: S3Info,
) -> None:
    """Deploy the minimal setup for Kyuubi and assert all charms are in active and idle state."""
    deploy_minimal_kyuubi_setup(
        juju=juju,
        kyuubi_charm=kyuubi_charm,
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=True,
    )

    # Wait for everything to settle down
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            APP_NAME,
            charm_versions.integration_hub.application_name,
            charm_versions.s3.application_name,
        ),
        delay=5,
    )


def test_dynamic_allocation_disabled(juju: jubilant.Juju) -> None:
    """Test running Kyuubi SQL queries when dynamic allocation option is disabled in Kyuubi charm."""
    status = juju.status()
    host = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address
    port = 10009

    # Put some load by executing some Kyuubi SQL queries
    kyuubi_client = KyuubiClient(host=host, port=int(port))
    db = kyuubi_client.get_database("default")
    table = db.create_table(name="table1", schema=[("id", int)])
    table.insert([55])

    # The number of executors is supposed to always be 2 by default
    n1 = len(get_spark_executors(namespace=cast(str, juju.model)))
    assert n1 == 2

    # Wait for some time of idleness, where there is no load in the Kyuubi
    time.sleep(90)

    # The number of executors is supposed to be the same (i.e. 2)
    n2 = len(get_spark_executors(namespace=cast(str, juju.model)))
    assert n2 == n1 == 2


def test_dynamic_allocation_enabled(juju: jubilant.Juju) -> None:
    """Test running Kyuubi SQL queries when dynamic allocation option is enabled in Kyuubi charm."""
    logger.info("Changing enable-dynamic-allocation to 'true' for kyuubi-k8s charm...")
    juju.config(APP_NAME, {"enable-dynamic-allocation": "true"})

    logger.info("Waiting for kyuubi-k8s app to be active and idle...")
    status = juju.wait(lambda status: jubilant.all_active(status, APP_NAME))

    host = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address
    port = 10009

    # Put some load by executing some Kyuubi SQL queries
    kyuubi_client = KyuubiClient(host=host, port=int(port))
    db = kyuubi_client.get_database("default")
    table = db.create_table(name="table2", schema=[("id", int)])
    table.insert([55])
    assert len(list(table.rows())) == 1

    # The load that was put earlier should have spawned at least one executor pod
    n1 = len(get_spark_executors(namespace=cast(str, juju.model)))
    assert n1 > 0

    # Wait for some time of idleness, where there is no load in the Kyuubi
    time.sleep(90)

    # The number of executor pods should now be zero, since there's no load to Kyuubi
    n2 = len(get_spark_executors(namespace=cast(str, juju.model)))
    assert n2 == 0

    # Put some load by executing some Kyuubi SQL queries again
    table.insert([77])
    assert len(list(table.rows())) == 2

    # The load that was put just now should have spawned at least one executor pod again
    n3 = len(get_spark_executors(namespace=cast(str, juju.model)))
    assert n3 > 0
