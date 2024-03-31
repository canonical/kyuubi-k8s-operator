# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import pytest
from ops import pebble
from scenario import Container, Context, Model, Mount, Relation
from scenario.state import next_relation_id

from charm import KyuubiCharm
from constants import KYUUBI_CONTAINER_NAME, S3_INTEGRATOR_REL


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

    opt = Mount("/opt/", tmp_path)
    etc = Mount("/etc", tmp_path)

    return Container(
        name=KYUUBI_CONTAINER_NAME,
        can_connect=True,
        layers={"base": layer},
        service_status={"kyuubi": pebble.ServiceStatus.ACTIVE},
        mounts={"opt": opt, "etc": etc},
    )


@pytest.fixture
def s3_relation():
    """Provide fixture for the S3 relation."""
    relation_id = next_relation_id(update=True)

    return Relation(
        endpoint=S3_INTEGRATOR_REL,
        interface="s3",
        remote_app_name="s3-integrator",
        relation_id=relation_id,
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
