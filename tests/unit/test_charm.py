#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path
from unittest.mock import Mock, patch

from ops.testing import Container, Context, PeerRelation, Relation, Secret, State

from constants import JOB_OCI_IMAGE, KYUUBI_CONTAINER_NAME
from core.domain import Status

logger = logging.getLogger(__name__)
SPARK_PROPERTIES = "/etc/spark8t/conf/spark-defaults.conf"
KYUUBI_PROPERTIES = "/opt/kyuubi/conf/kyuubi-defaults.conf"


def parse_spark_properties(tmp_path: Path) -> dict[str, str]:
    """Parse and return spark properties from the conf file in the container."""
    file_path = tmp_path / Path(SPARK_PROPERTIES).relative_to("/etc")
    with file_path.open("r") as fid:
        return dict(
            row.rsplit("=", maxsplit=1) for line in fid.readlines() if (row := line.strip())
        )


def parse_kyuubi_configurations(tmp_path: Path) -> dict[str, str]:
    """Parse and return Kyuubi configurations from the conf file in the container."""
    file_path = tmp_path / Path(KYUUBI_PROPERTIES).relative_to("/opt")
    with file_path.open("r") as fid:
        return dict(
            row.rsplit("=", maxsplit=1) for line in fid.readlines() if (row := line.strip())
        )


def test_start_kyuubi(kyuubi_context: Context) -> None:
    state = State(
        config={},
        containers=[Container(name=KYUUBI_CONTAINER_NAME, can_connect=False)],
    )
    out = kyuubi_context.run(kyuubi_context.on.install(), state)
    assert out.unit_status == Status.WAITING_PEBBLE.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("config.spark.SparkConfig._sa_conf", return_value={})
def test_pebble_ready(
    mock_sa_conf,
    mock_get_master,
    mock_valid_sa,
    mock_valid_ns,
    kyuubi_context: Context,
    kyuubi_container: Container,
) -> None:
    state = State(
        containers=[kyuubi_container],
    )
    out = kyuubi_context.run(kyuubi_context.on.pebble_ready(kyuubi_container), state)
    assert out.unit_status == Status.MISSING_INTEGRATION_HUB.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.service.ServiceManager.get_service_endpoint", return_value="")
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
@patch("config.spark.SparkConfig._sa_conf", return_value={})
def test_service_unavailable(
    mock_sa_conf,
    mock_s3_configured,
    mock_get_master,
    mock_service_endpoint,
    mock_valid_sa,
    mock_valid_ns,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    auth_db_relation: Relation,
) -> None:
    state = State(
        relations=[spark_service_account_relation, auth_db_relation],
        containers=[kyuubi_container],
    )
    out = kyuubi_context.run(kyuubi_context.on.config_changed(), state)
    assert out.unit_status == Status.WAITING_FOR_SERVICE.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
@patch("config.spark.SparkConfig._sa_conf", return_value={})
def test_system_users_config_secret_doesnot_exist(
    mock_sa_conf,
    mock_s3_configured,
    mock_get_master,
    mock_valid_sa,
    mock_valid_ns,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    auth_db_relation: Relation,
) -> None:
    """Test when the admin password supplied via system-users config option does not exist."""
    system_users_secret = Secret(tracked_content={"admin": "password"})
    state = State(
        relations=[spark_service_account_relation, auth_db_relation],
        containers=[kyuubi_container],
        config={"system-users": system_users_secret.id},
    )
    out = kyuubi_context.run(kyuubi_context.on.config_changed(), state)
    assert out.unit_status == Status.SYSTEM_USERS_SECRET_DOES_NOT_EXIST.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
@patch("config.spark.SparkConfig._sa_conf", return_value={})
def test_system_users_config_secret_invalid(
    mock_sa_conf,
    mock_s3_configured,
    mock_get_master,
    mock_valid_sa,
    mock_valid_ns,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    auth_db_relation: Relation,
) -> None:
    """Test when the admin password supplied via system-users config option is invalid."""
    system_users_secret = Secret(tracked_content={"some-user": "password"})
    state = State(
        relations=[spark_service_account_relation, auth_db_relation],
        containers=[kyuubi_container],
        config={"system-users": system_users_secret.id},
        secrets=[system_users_secret],
    )
    out = kyuubi_context.run(kyuubi_context.on.config_changed(), state)
    assert out.unit_status == Status.SYSTEM_USERS_SECRET_INVALID.value


@patch("managers.auth.AuthenticationManager.set_password")
@patch("managers.auth.AuthenticationManager.user_exists", return_value=True)
@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch(
    "events.provider.KyuubiClientProviderEvents.update_clients_endpoints",
    return_value=True,
)
@patch(
    "managers.service.ServiceManager.get_service_endpoint",
    return_value="10.10.10.10:10009",
)
@patch(
    "managers.service.ServiceManager.reconcile_services",
    return_value=True,
)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
@patch(
    "config.spark.SparkConfig._sa_conf", return_value={"spark.hadoop.fs.s3a.endpoint": "foo.bar"}
)
def test_system_users_config_secret_valid(
    mock_sa_conf,
    mock_s3_configured,
    mock_get_master,
    mock_reconcile_service,
    mock_service_endpoint,
    mock_update_client_endpoints,
    mock_valid_sa,
    mock_valid_ns,
    mock_user_exists,
    mock_set_password,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    auth_db_relation: Relation,
    kyuubi_peers_relation: PeerRelation,
) -> None:
    """Test when the admin password supplied via system-users config option is valid."""
    system_users_secret = Secret(tracked_content={"admin": "password"})
    state = State(
        relations=[spark_service_account_relation, auth_db_relation, kyuubi_peers_relation],
        containers=[kyuubi_container],
        config={"system-users": system_users_secret.id},
        secrets=[system_users_secret],
        leader=True,
    )
    out = kyuubi_context.run(kyuubi_context.on.config_changed(), state)
    assert out.unit_status == Status.ACTIVE.value

    assert mock_set_password.called
    _, kwargs = mock_set_password.call_args
    assert kwargs["username"] == "admin"
    assert kwargs["password"] == "password"

    peer_app_secret = [
        secret for secret in out.secrets if secret.label == "kyuubi-peers.kyuubi-k8s.app"
    ]
    assert len(peer_app_secret) > 0
    peer_app_secret_content = peer_app_secret[0].latest_content
    assert peer_app_secret_content is not None
    assert peer_app_secret_content["admin-password"] == "password"


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
@patch(
    "managers.service.ServiceManager.get_service_endpoint",
    return_value="10.10.10.10:10009",
)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch(
    "config.spark.SparkConfig._sa_conf", return_value={"spark.hadoop.fs.s3a.endpoint": "foo.bar"}
)
def test_valid_on_service_account(
    mock_sa_conf,
    mock_get_master,
    mock_service_endpoint,
    mock_s3_configured,
    mock_valid_sa,
    mock_valid_ns,
    tmp_path,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    auth_db_relation: Relation,
) -> None:
    state = State(
        relations=[spark_service_account_relation, auth_db_relation],
        containers=[kyuubi_container],
    )
    out = kyuubi_context.run(
        kyuubi_context.on.relation_changed(spark_service_account_relation), state
    )
    assert out.unit_status == Status.ACTIVE.value

    # Check containers modifications
    assert len(out.get_container(KYUUBI_CONTAINER_NAME).layers) == 1

    spark_properties = parse_spark_properties(tmp_path)
    logger.info(spark_properties)

    # Assert one of the keys
    assert "spark.hadoop.fs.s3a.endpoint" in spark_properties
    assert spark_properties["spark.hadoop.fs.s3a.endpoint"] == "foo.bar"


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=False)
@patch("managers.k8s.K8sManager.is_azure_storage_configured", return_value=False)
@patch(
    "managers.service.ServiceManager.get_service_endpoint",
    return_value="10.10.10.10:10009",
)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("config.spark.SparkConfig._sa_conf", return_value={"foo": "bar"})
def test_object_storage_backend_removed(
    mock_sa_conf,
    mock_get_master,
    mock_service_endpoint,
    mock_azure_configured,
    mock_s3_configured,
    mock_valid_sa,
    mock_valid_ns,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
) -> None:
    initial_state = State(
        relations=[spark_service_account_relation],
        containers=[kyuubi_container],
    )
    out = kyuubi_context.run(
        kyuubi_context.on.relation_changed(spark_service_account_relation), initial_state
    )

    assert out.unit_status == Status.MISSING_OBJECT_STORAGE_BACKEND.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
@patch(
    "managers.service.ServiceManager.get_service_endpoint",
    return_value="10.10.10.10:10009",
)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("config.spark.SparkConfig._sa_conf", return_value={})
def test_zookeeper_relation_joined(
    mock_sa_conf,
    mock_get_master,
    mock_service_endpoint,
    mock_s3_configured,
    mock_valid_sa,
    mock_valid_ns,
    tmp_path,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    auth_db_relation: Relation,
    zookeeper_relation: Relation,
):
    state = State(
        relations=[spark_service_account_relation, auth_db_relation, zookeeper_relation],
        containers=[kyuubi_container],
    )
    out = kyuubi_context.run(kyuubi_context.on.relation_changed(zookeeper_relation), state)
    assert out.unit_status == Status.ACTIVE.value

    kyuubi_configurations = parse_kyuubi_configurations(tmp_path)

    # Assert some of the keys
    assert (
        kyuubi_configurations["kyuubi.ha.namespace"]
        == zookeeper_relation.remote_app_data["database"]
    )
    assert (
        kyuubi_configurations["kyuubi.ha.addresses"] == zookeeper_relation.remote_app_data["uris"]
    )
    assert kyuubi_configurations["kyuubi.ha.zookeeper.auth.type"] == "DIGEST"
    assert (
        kyuubi_configurations["kyuubi.ha.zookeeper.auth.digest"]
        == f"{zookeeper_relation.remote_app_data['username']}:{zookeeper_relation.remote_app_data['password']}"
    )


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
@patch(
    "managers.service.ServiceManager.get_service_endpoint",
    return_value="10.10.10.10:10009",
)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("config.spark.SparkConfig._sa_conf", return_value={})
def test_zookeeper_relation_broken(
    mock_sa_conf,
    mock_get_master,
    mock_service_endpoint,
    mock_s3_configured,
    mock_valid_sa,
    mock_valid_ns,
    tmp_path,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    auth_db_relation: Relation,
    zookeeper_relation: Relation,
) -> None:
    state = State(
        relations=[spark_service_account_relation, auth_db_relation, zookeeper_relation],
        containers=[kyuubi_container],
    )
    state_after_relation_changed = kyuubi_context.run(
        kyuubi_context.on.relation_changed(zookeeper_relation), state
    )
    state_after_relation_broken = kyuubi_context.run(
        kyuubi_context.on.relation_broken(
            state_after_relation_changed.get_relation(zookeeper_relation.id)
        ),
        state_after_relation_changed,
    )
    assert state_after_relation_broken.unit_status == Status.ACTIVE.value

    kyuubi_configurations = parse_kyuubi_configurations(tmp_path)

    # Assert HA configurations do not exist in Kyuubi configuration file
    assert "kyuubi.ha.namespace" not in kyuubi_configurations
    assert "kyuubi.ha.addresses" not in kyuubi_configurations
    assert "kyuubi.ha.zookeeper.auth.type" not in kyuubi_configurations
    assert "kyuubi.ha.zookeeper.auth.digest" not in kyuubi_configurations


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
@patch(
    "managers.service.ServiceManager.get_service_endpoint",
    return_value="10.10.10.10:10009",
)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("config.spark.SparkConfig._sa_conf", return_value={})
def test_spark_service_account_broken(
    mock_sa_conf,
    mock_get_master,
    mock_service_endpoint,
    mock_s3_configured,
    mock_valid_sa,
    mock_valid_ns,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
) -> None:
    initial_state = State(
        relations=[spark_service_account_relation],
        containers=[kyuubi_container],
    )

    state_after_relation_changed = kyuubi_context.run(
        kyuubi_context.on.relation_changed(spark_service_account_relation), initial_state
    )
    state_after_relation_broken = kyuubi_context.run(
        kyuubi_context.on.relation_broken(
            state_after_relation_changed.get_relation(spark_service_account_relation.id)
        ),
        state_after_relation_changed,
    )

    assert state_after_relation_broken.unit_status == Status.MISSING_INTEGRATION_HUB.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=False)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("config.spark.SparkConfig._sa_conf", return_value={})
def test_invalid_namespace(
    mock_sa_conf,
    mock_get_master,
    mock_s3_configured,
    mock_valid_sa,
    mock_valid_ns,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
):
    state = State(
        relations=[spark_service_account_relation],
        containers=[kyuubi_container],
    )
    out = kyuubi_context.run(kyuubi_context.on.pebble_ready(kyuubi_container), state)
    assert out.unit_status == Status.INVALID_NAMESPACE.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=False)
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("config.spark.SparkConfig._sa_conf", return_value={})
def test_invalid_service_account(
    mock_sa_conf,
    mock_get_master,
    mock_s3_configured,
    mock_valid_sa,
    mock_valid_ns,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
) -> None:
    state = State(
        relations=[spark_service_account_relation],
        containers=[kyuubi_container],
    )
    out = kyuubi_context.run(kyuubi_context.on.pebble_ready(kyuubi_container), state)
    assert out.unit_status == Status.INVALID_SERVICE_ACCOUNT.value


@patch("ops.model.Application.planned_units", return_value=3)
@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("config.spark.SparkConfig._sa_conf", return_value={})
def test_missing_zookeeper_for_multiple_units_of_kyuubi(
    mock_sa_conf,
    mock_get_master,
    mock_s3_configured,
    mock_valid_sa,
    mock_valid_ns,
    mock_planned_units,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    auth_db_relation: Relation,
) -> None:
    state = State(
        relations=[spark_service_account_relation, auth_db_relation],
        containers=[kyuubi_container],
    )
    out = kyuubi_context.run(kyuubi_context.on.pebble_ready(kyuubi_container), state)
    assert out.unit_status == Status.MISSING_ZOOKEEPER.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
@patch(
    "managers.service.ServiceManager.get_service_endpoint",
    return_value="10.10.10.10:10009",
)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch(
    "config.spark.SparkConfig._sa_conf",
    return_value={
        "new_property": "new_value",
        "spark.kubernetes.container.image": "image_from_service_account",
    },
)
def test_spark_property_priorities(
    mock_sa_conf,
    mock_get_master,
    mock_get_service_endpoint,
    mock_s3_configured,
    mock_valid_sa,
    mock_valid_ns,
    tmp_path,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    auth_db_relation: Relation,
):
    state = State(
        relations=[spark_service_account_relation, auth_db_relation],
        containers=[kyuubi_container],
    )
    out = kyuubi_context.run(
        kyuubi_context.on.relation_changed(spark_service_account_relation), state
    )
    assert out.unit_status == Status.ACTIVE.value

    spark_properties = parse_spark_properties(tmp_path)

    # New property read from service account (via spark8t) should
    # appear in the Spark properties file
    assert spark_properties["new_property"] == "new_value"

    # Property read from service account (via spark8t) should
    # override the property of same name set by Kyuubi charm.
    assert spark_properties["spark.kubernetes.container.image"] != JOB_OCI_IMAGE
    assert spark_properties["spark.kubernetes.container.image"] == "image_from_service_account"


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
@patch("config.spark.SparkConfig._get_spark_master", return_value="k8s://https://spark.master")
@patch("config.spark.SparkConfig._sa_conf", return_value={})
@patch("managers.service.ServiceManager.get_service_endpoint", return_value=True)
def test_kyuubi_not_serving_requests(
    mock_k8s_service,
    mock_sa_conf,
    mock_get_master,
    mock_s3_configured,
    mock_valid_sa,
    mock_valid_ns,
    kyuubi_context: Context,
    kyuubi_container: Container,
    spark_service_account_relation: Relation,
    auth_db_relation: Relation,
    mock_socket_connect: Mock,
) -> None:
    """This test simulates a running pebble service not actively serving requests.

    This can happen if the keystore file is missing, the service would loop restart.
    """
    # Given
    mock_socket_connect.return_value = False
    state = State(
        relations=[spark_service_account_relation, auth_db_relation],
        containers=[kyuubi_container],
    )

    # When
    out = kyuubi_context.run(kyuubi_context.on.update_status(), state)

    # Then
    assert out.unit_status == Status.NOT_SERVING_REQUESTS.value
