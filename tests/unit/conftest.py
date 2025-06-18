# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import shutil
from subprocess import check_output
from unittest.mock import Mock, patch

import pytest
from ops import pebble
from ops.testing import Container, Context, Model, Mount, Relation

from charm import KyuubiCharm
from constants import (
    KYUUBI_CONTAINER_NAME,
    POSTGRESQL_AUTH_DB_REL,
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
    return Context(charm_type=kyuubi_charm, app_trusted=True)


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
def spark_service_account_relation():
    """Provide fixture for the spark-service-account relation."""
    return Relation(
        endpoint=SPARK_SERVICE_ACCOUNT_REL,
        interface="spark-service-account",
        remote_app_name="integration-hub",
        local_app_data={"service-account": "spark:kyuubi", "spark-properties": "{'foo':'bar'}"},
        remote_app_data={"service-account": "spark:kyuubi", "spark-properties": '{"foo":"bar"}'},
    )


@pytest.fixture
def auth_db_relation() -> Relation:
    return Relation(
        endpoint=POSTGRESQL_AUTH_DB_REL,
        interface="postgresql_client",
        remote_app_name="kyuubi-users",
        local_app_data={
            "database": "kyuubi-users",
        },
        remote_app_data={
            "database": "myappB",
            "endpoints": "postgresql-k8s-primary:5432",
            "username": "kyuubi",
            "password": "pwd",
        },
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


@pytest.fixture(autouse=True)
def mock_socket_connect():
    """A fixture to run unit tests even in non K8s environment."""
    with patch("core.workload.kyuubi.KyuubiWorkload.serving_requests") as patched:
        yield patched


@pytest.fixture(autouse=True)
def mock_refresh():
    """Fixture to shunt refresh logic and events."""
    with (
        patch("charm_refresh.Kubernetes", Mock(return_value=None)),
        patch("charm.KyuubiRefresh", Mock(return_value=None)),
    ):
        yield


@pytest.fixture(scope="session")
def skopeo() -> str:
    """Check that skopeo is in path and runnable."""
    if (skopeo_path := shutil.which("skopeo")) is None:
        if (skopeo_path := shutil.which("rockcraft.skopeo")) is None:
            raise FileNotFoundError("Could not find 'skopeo' in PATH.")

    check_output([skopeo_path, "-v"])
    return skopeo_path
