#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path
from unittest.mock import patch

from scenario import Container, State

from constants import KYUUBI_CONTAINER_NAME, KYUUBI_OCI_IMAGE, SPARK_PROPERTIES_FILE
from core.domain import Status

logger = logging.getLogger(__name__)


def parse_spark_properties(tmp_path: Path) -> dict[str, str]:
    """Parse and return spark properties from the conf file in the container."""
    file_path = tmp_path / Path(SPARK_PROPERTIES_FILE).relative_to("/etc")
    with file_path.open("r") as fid:
        return dict(
            row.rsplit("=", maxsplit=1) for line in fid.readlines() if (row := line.strip())
        )


def test_start_kyuubi(kyuubi_context):
    state = State(
        config={},
        containers=[Container(name=KYUUBI_CONTAINER_NAME, can_connect=False)],
    )
    out = kyuubi_context.run("install", state)
    assert out.unit_status == Status.WAITING_PEBBLE.value


def test_pebble_ready(kyuubi_context, kyuubi_container):
    state = State(
        containers=[kyuubi_container],
    )
    out = kyuubi_context.run(kyuubi_container.pebble_ready_event, state)
    assert out.unit_status == Status.MISSING_S3_RELATION.value


@patch("managers.s3.S3Manager.verify", return_value=False)
@patch("utils.k8s.is_valid_namespace", return_value=True)
@patch("utils.k8s.is_valid_service_account", return_value=True)
def test_s3_relation_invalid_credentials(
    mock_valid_sa, mock_valid_ns, mock_s3_verify, kyuubi_context, kyuubi_container, s3_relation
):
    state = State(
        relations=[s3_relation],
        containers=[kyuubi_container],
    )
    out = kyuubi_context.run(s3_relation.changed_event, state)
    assert out.unit_status == Status.INVALID_CREDENTIALS.value


@patch("s3.S3ConnectionInfo.verify", return_value=True)
@patch("utils.k8s.is_valid_namespace", return_value=True)
@patch("utils.k8s.is_valid_service_account", return_value=True)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("config.spark.SparkConfig._sa_conf", return_value={})
def test_s3_relation_valid_credentials(
    mock_sa_conf,
    mock_get_master,
    mock_valid_sa,
    mock_valid_ns,
    mock_s3_verify,
    tmp_path,
    kyuubi_context,
    kyuubi_container,
    s3_relation,
):
    state = State(
        relations=[s3_relation],
        containers=[kyuubi_container],
    )
    out = kyuubi_context.run(s3_relation.changed_event, state)
    assert out.unit_status == Status.ACTIVE.value

    # Check containers modifications
    assert len(out.get_container(KYUUBI_CONTAINER_NAME).layers) == 1

    spark_properties = parse_spark_properties(tmp_path)
    logger.info(spark_properties)
    # Assert one of the keys
    assert "spark.hadoop.fs.s3a.endpoint" in spark_properties
    assert (
        spark_properties["spark.hadoop.fs.s3a.endpoint"] == s3_relation.remote_app_data["endpoint"]
    )


@patch("s3.S3ConnectionInfo.verify", return_value=True)
@patch("utils.k8s.is_valid_namespace", return_value=True)
@patch("utils.k8s.is_valid_service_account", return_value=True)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("config.spark.SparkConfig._sa_conf", return_value={})
def test_s3_relation_broken(
    mock_sa_conf,
    mock_get_master,
    mock_valid_sa,
    mock_valid_ns,
    mock_s3_verify,
    kyuubi_context,
    kyuubi_container,
    s3_relation,
):
    initial_state = State(
        relations=[s3_relation],
        containers=[kyuubi_container],
    )

    state_after_relation_changed = kyuubi_context.run(s3_relation.changed_event, initial_state)
    state_after_relation_broken = kyuubi_context.run(
        s3_relation.broken_event, state_after_relation_changed
    )

    assert state_after_relation_broken.unit_status == Status.MISSING_S3_RELATION.value


@patch("s3.S3ConnectionInfo.verify", return_value=True)
@patch("utils.k8s.is_valid_namespace", return_value=False)
@patch("utils.k8s.is_valid_service_account", return_value=True)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
def test_invalid_namespace(
    mock_get_master,
    mock_valid_sa,
    mock_valid_ns,
    mock_s3_verify,
    kyuubi_context,
    kyuubi_container,
    s3_relation,
):
    state = State(
        relations=[s3_relation],
        containers=[kyuubi_container],
    )
    out = kyuubi_context.run(kyuubi_container.pebble_ready_event, state)
    assert out.unit_status == Status.INVALID_NAMESPACE.value


@patch("s3.S3ConnectionInfo.verify", return_value=True)
@patch("utils.k8s.is_valid_namespace", return_value=True)
@patch("utils.k8s.is_valid_service_account", return_value=False)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
def test_invalid_service_account(
    mock_get_master,
    mock_valid_sa,
    mock_valid_ns,
    mock_s3_verify,
    kyuubi_context,
    kyuubi_container,
    s3_relation,
):
    state = State(
        relations=[s3_relation],
        containers=[kyuubi_container],
    )
    out = kyuubi_context.run(kyuubi_container.pebble_ready_event, state)
    assert out.unit_status == Status.INVALID_SERVICE_ACCOUNT.value


@patch("s3.S3ConnectionInfo.verify", return_value=True)
@patch("utils.k8s.is_valid_namespace", return_value=True)
@patch("utils.k8s.is_valid_service_account", return_value=True)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch(
    "config.spark.SparkConfig._sa_conf",
    return_value={
        "new_property": "new_value",
        "spark.kubernetes.container.image": "image_from_service_account",
        "spark.hadoop.fs.s3a.endpoint": "endpoint_from_service_account",
    },
)
def test_spark_property_priorities(
    mock_sa_conf,
    mock_get_master,
    mock_valid_sa,
    mock_valid_ns,
    mock_s3_verify,
    tmp_path,
    kyuubi_context,
    kyuubi_container,
    s3_relation,
):
    state = State(
        relations=[s3_relation],
        containers=[kyuubi_container],
    )
    kyuubi_context.run(s3_relation.changed_event, state)

    spark_properties = parse_spark_properties(tmp_path)

    # New property read from service account (via spark8t) should
    # appear in the Spark properties file
    assert spark_properties["new_property"] == "new_value"

    # Property read from service account (via spark8t) should
    # override the property of same name set by Kyuubi charm.
    assert spark_properties["spark.kubernetes.container.image"] != KYUUBI_OCI_IMAGE
    assert spark_properties["spark.kubernetes.container.image"] == "image_from_service_account"

    # However, property created by charm relation should override
    # property read from service account (via spark8t)
    assert spark_properties["spark.hadoop.fs.s3a.endpoint"] != "endpoint_from_service_account"
    assert (
        spark_properties["spark.hadoop.fs.s3a.endpoint"] == s3_relation.remote_app_data["endpoint"]
    )
