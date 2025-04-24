#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import pytest
import yaml
from pytest_operator.plugin import OpsTest
from spark_test.core.kyuubi import KyuubiClient
from spark_test.utils import get_spark_executors

from core.domain import Status

from .helpers import (
    check_status,
    deploy_minimal_kyuubi_setup,
    get_address,
    juju_sleep,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


@pytest.mark.abort_on_fail
async def test_deploy_kyuubi_setup(
    ops_test: OpsTest,
    kyuubi_charm: Path,
    charm_versions,
    s3_bucket_and_creds,
) -> None:
    """Deploy the minimal setup for Kyuubi and assert all charms are in active and idle state."""
    await deploy_minimal_kyuubi_setup(
        ops_test=ops_test,
        kyuubi_charm=kyuubi_charm,
        charm_versions=charm_versions,
        s3_bucket_and_creds=s3_bucket_and_creds,
        trust=True,
    )

    # Wait for everything to settle down
    await ops_test.model.wait_for_idle(
        apps=[
            APP_NAME,
            charm_versions.integration_hub.application_name,
            charm_versions.s3.application_name,
        ],
        idle_period=20,
        status="active",
    )

    # Assert that all charms that were deployed as part of minimal setup are in correct states.
    assert check_status(ops_test.model.applications[APP_NAME], Status.ACTIVE.value)
    assert (
        ops_test.model.applications[charm_versions.integration_hub.application_name].status
        == "active"
    )
    assert ops_test.model.applications[charm_versions.s3.application_name].status == "active"


@pytest.mark.abort_on_fail
async def test_dynamic_allocation_disabled(ops_test):
    """Test running Kyuubi SQL queries when dynamic allocation option is disabled in Kyuubi charm."""
    host = await get_address(ops_test, unit_name=f"{APP_NAME}/0")
    port = 10009

    # Put some load by executing some Kyuubi SQL queries
    kyuubi_client = KyuubiClient(host=host, port=int(port))
    db = kyuubi_client.get_database("default")
    table = db.create_table(name="table1", schema=[("id", int)])
    table.insert([55])

    # The number of executors is supposed to always be 2 by default
    n1 = len(get_spark_executors(namespace=ops_test.model.name))
    assert n1 == 2

    # Wait for some time of idleness, where there is no load in the Kyuubi
    await juju_sleep(ops_test, 90, APP_NAME)

    # The number of executors is supposed to be the same (i.e. 2)
    n2 = len(get_spark_executors(namespace=ops_test.model.name))
    assert n2 == n1 == 2


@pytest.mark.abort_on_fail
async def test_dynamic_allocation_enabled(ops_test):
    """Test running Kyuubi SQL queries when dynamic allocation option is enabled in Kyuubi charm."""
    logger.info("Changing enable-dynamic-allocation to 'true' for kyuubi-k8s charm...")
    await ops_test.model.applications[APP_NAME].set_config({"enable-dynamic-allocation": "true"})

    logger.info("Waiting for kyuubi-k8s app to be active and idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )

    host = await get_address(ops_test, unit_name="kyuubi-k8s/0")
    port = 10009

    # Put some load by executing some Kyuubi SQL queries
    kyuubi_client = KyuubiClient(host=host, port=int(port))
    db = kyuubi_client.get_database("default")
    table = db.create_table(name="table2", schema=[("id", int)])
    table.insert([55])
    assert len(list(table.rows())) == 1

    # The load that was put earlier should have spawned at least one executor pod
    n1 = len(get_spark_executors(namespace=ops_test.model.name))
    assert n1 > 0

    # Wait for some time of idleness, where there is no load in the Kyuubi
    await juju_sleep(ops_test, 90, APP_NAME)

    # The number of executor pods should now be zero, since there's no load to Kyuubi
    n2 = len(get_spark_executors(namespace=ops_test.model.name))
    assert n2 == 0

    # Put some load by executing some Kyuubi SQL queries again
    table.insert([77])
    assert len(list(table.rows())) == 2

    # The load that was put just now should have spawned at least one executor pod again
    n3 = len(get_spark_executors(namespace=ops_test.model.name))
    assert n3 > 0
