# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

from dataclasses import replace
from unittest.mock import patch

import pytest
from ops import pebble
from ops.testing import Container, Context, Model, Mount, Relation

from charm import KyuubiCharm
from constants import (
    KYUUBI_CONTAINER_NAME,
    S3_INTEGRATOR_REL,
    SPARK_SERVICE_ACCOUNT_REL,
    ZOOKEEPER_REL,
)


@pytest.fixture
def kyuubi_charm():
    """Provide fixture for the Kyuubi charm."""
    yield KyuubiCharm


@pytest.fixture
def kyuubi_context(kyuubi_charm):
    """Provide fixture for scenario context based on the Kyuubi charm."""
    return Context(charm_type=kyuubi_charm)


@pytest.fixture
def model():
    """Provide fixture for the testing Juju model."""
    return Model(name="test-model")


@pytest.fixture
def kyuubi_container(tmp_path):
    """Provide fixture for the Kyuubi workload container."""
    layer = pebble.Layer(
        {
            "summary": "kyuubi layer",
            "description": "pebble config layer for kyuubi",
            "services": {
                "kyuubi": {
                    "override": "merge",
                    "summary": "This is the Kyuubi service",
                    "command": "/bin/bash /opt/pebble/kyuubi.sh",
                    "startup": "enabled",
                },
            },
        }
    )

    opt = Mount(location="/opt/", source=tmp_path)
    etc = Mount(location="/etc", source=tmp_path)

    return Container(
        name=KYUUBI_CONTAINER_NAME,
        can_connect=True,
        layers={"base": layer},
        service_statuses={"kyuubi": pebble.ServiceStatus.ACTIVE},
        mounts={"opt": opt, "etc": etc},
    )


@pytest.fixture
def s3_relation():
    """Provide fixture for the S3 relation."""
    relation = Relation(
        endpoint=S3_INTEGRATOR_REL,
        interface="s3",
        remote_app_name="s3-integrator",
    )
    relation_id = relation.id

    return replace(
        relation,
        local_app_data={"bucket": f"relation-{relation_id}"},
        remote_app_data={
            "access-key": "access-key",
            "bucket": "my-bucket",
            "data": f'{{"bucket": "relation-{relation_id}"}}',
            "endpoint": "https://s3.endpoint",
            "path": "spark-events",
            "secret-key": "secret-key",
        },
    )


@pytest.fixture
def spark_service_account_relation():
    """Provide fixture for the S3 relation."""
    return Relation(
        endpoint=SPARK_SERVICE_ACCOUNT_REL,
        interface="spark-service-account",
        remote_app_name="integration-hub",
        local_app_data={"service-account": "kyuubi", "namespace": "spark"},
        remote_app_data={"service-account": "kyuubi", "namespace": "spark"},
    )


@pytest.fixture
def zookeeper_relation():
    """Provide fixture for the Zookeeper relation."""
    return Relation(
        endpoint=ZOOKEEPER_REL,
        interface="zookeeper",
        remote_app_name="zookeeper-k8s",
        local_app_data={"database": "/kyuubi"},
        remote_app_data={
            "uris": "host1:2181,host2:2181,host3:2181",
            "username": "foobar",
            "password": "foopassbarword",
            "database": "/kyuubi",
        },
    )


@pytest.fixture(autouse=True)
def mock_lightkube_client():
    """A fixture to run unit tests even in non K8s environment."""
    with patch("lightkube.Client") as mock_client:
        yield mock_client
