#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import json
from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import pytest
from ops.testing import Container, Context, PeerRelation, Relation, State

if TYPE_CHECKING:
    from charm import KyuubiCharm

SERVICE_MESH_RELATION = "service-mesh"
LABEL_CONFIGMAP_NAME = "juju-service-mesh-kyuubi-k8s-labels"
BEACON_LABELS = {"istio.io/dataplane-mode": "ambient"}


def _mesh_relation() -> Relation:
    return Relation(
        endpoint=SERVICE_MESH_RELATION,
        interface="service_mesh",
        remote_app_name="istio-beacon-k8s",
        remote_app_data={
            "labels": json.dumps(BEACON_LABELS),
            "mesh_type": json.dumps("istio"),
        },
    )


@patch("charm.KyuubiCharm.refresh_ready_for_service_mesh", return_value=True)
@patch("lightkube.Client")
@patch("charmlibs.interfaces.service_mesh._service_mesh.reconcile_charm_labels")
@patch("core.workload.kyuubi.KyuubiWorkload.exec")
def test_service_mesh_relation_adds_labels(
    exec_calls,
    reconcile_charm_labels,
    client_class,
    refresh_ready,
    kyuubi_context: "Context[KyuubiCharm]",
    kyuubi_container: Container,
) -> None:
    """Ensure that joining the service-mesh relation applies the beacon's ambient labels to Kyuubi."""
    beacon_labels = {"istio.io/dataplane-mode": "ambient"}
    mesh_relation = Relation(
        endpoint=SERVICE_MESH_RELATION,
        interface="service_mesh",
        remote_app_name="istio-beacon-k8s",
        remote_app_data={
            "labels": json.dumps(beacon_labels),
            "mesh_type": json.dumps("istio"),
        },
    )
    state = State(
        leader=True,
        relations=[mesh_relation],
        containers=[kyuubi_container],
    )

    kyuubi_context.run(kyuubi_context.on.relation_changed(mesh_relation), state)

    reconcile_charm_labels.assert_called_once()
    assert reconcile_charm_labels.call_args.kwargs["labels"] == beacon_labels
    assert reconcile_charm_labels.call_args.kwargs["label_configmap_name"] == LABEL_CONFIGMAP_NAME


@patch("events.service_mesh.Client")
@patch("lightkube.Client")
@patch("charmlibs.interfaces.service_mesh._service_mesh.reconcile_charm_labels")
@patch("core.workload.kyuubi.KyuubiWorkload.exec")
def test_service_mesh_relation_broken_removes_labels(
    exec_calls,
    reconcile_charm_labels,
    client_class,
    mesh_client_class,
    kyuubi_context: "Context[KyuubiCharm]",
    kyuubi_container: Container,
) -> None:
    """Ensure that breaking the service-mesh relation clears the ambient labels from Kyuubi."""
    mesh_relation = Relation(
        endpoint=SERVICE_MESH_RELATION,
        interface="service_mesh",
        remote_app_name="istio-beacon-k8s",
        remote_app_data={
            "labels": json.dumps({"istio.io/dataplane-mode": "ambient"}),
            "mesh_type": json.dumps("istio"),
        },
    )
    state = State(
        leader=True,
        relations=[mesh_relation],
        containers=[kyuubi_container],
    )

    kyuubi_context.run(kyuubi_context.on.relation_broken(mesh_relation), state)

    reconcile_charm_labels.assert_called_once()
    assert reconcile_charm_labels.call_args.kwargs["labels"] == {}
    assert reconcile_charm_labels.call_args.kwargs["label_configmap_name"] == LABEL_CONFIGMAP_NAME
    # relation-broken also removes the configmap that tracks previously-applied labels.
    client_class.return_value.delete.assert_called_once()


@patch("charm.KyuubiCharm.refresh_ready_for_service_mesh")
@patch("lightkube.Client")
@patch("charmlibs.interfaces.service_mesh._service_mesh.reconcile_charm_labels")
@patch("core.workload.kyuubi.KyuubiWorkload.exec")
def test_service_mesh_labels_deferred_until_refresh_ready(
    exec_calls,
    reconcile_charm_labels,
    client_class,
    refresh_ready,
    kyuubi_context: "Context[KyuubiCharm]",
    kyuubi_container: Container,
) -> None:
    """WORKAROUND: labels must not be applied before charm-refresh saved the original versions."""
    mesh_relation = _mesh_relation()
    state = State(leader=True, relations=[mesh_relation], containers=[kyuubi_container])

    # Given charm-refresh is not ready yet
    refresh_ready.return_value = False
    out = kyuubi_context.run(kyuubi_context.on.relation_changed(mesh_relation), state)

    # Then the labels are not applied and the event is deferred
    reconcile_charm_labels.assert_not_called()
    assert [e.name for e in out.deferred] == [
        f"{SERVICE_MESH_RELATION.replace('-', '_')}_relation_changed"
    ]

    # When charm-refresh becomes ready, the deferred event is replayed on the next hook
    refresh_ready.return_value = True
    out = kyuubi_context.run(kyuubi_context.on.leader_elected(), out)

    reconcile_charm_labels.assert_called_once()
    assert reconcile_charm_labels.call_args.kwargs["labels"] == BEACON_LABELS
    assert not out.deferred


@pytest.mark.parametrize(
    "in_progress,app_data,expected",
    [
        (False, {"original_workload_container_version": '"sha256:abc"'}, True),
        (False, {}, False),
        (True, {"original_workload_container_version": '"sha256:abc"'}, False),
    ],
)
@patch("core.workload.kyuubi.KyuubiWorkload.exec")
def test_refresh_ready_for_service_mesh(
    exec_calls,
    in_progress: bool,
    app_data: dict[str, str],
    expected: bool,
    kyuubi_context: "Context[KyuubiCharm]",
    kyuubi_container: Container,
) -> None:
    """WORKAROUND: readiness requires an idle charm-refresh and saved original versions."""
    peer = PeerRelation(endpoint="refresh-v-three", interface="refresh", local_app_data=app_data)
    state = State(leader=True, relations=[peer], containers=[kyuubi_container])
    refresh = Mock(
        in_progress=in_progress,
        workload_allowed_to_start=False,
        unit_status_higher_priority=None,
        app_status_higher_priority=None,
    )
    refresh.unit_status_lower_priority.return_value = None

    with patch("charm_refresh.Kubernetes", Mock(return_value=refresh)):
        with kyuubi_context(kyuubi_context.on.leader_elected(), state) as manager:
            assert manager.charm.refresh_ready_for_service_mesh() is expected


def test_refresh_ready_for_service_mesh_without_refresh(
    kyuubi_context: "Context[KyuubiCharm]",
    kyuubi_container: Container,
) -> None:
    """WORKAROUND: not ready while charm-refresh could not be initialised."""
    state = State(leader=True, containers=[kyuubi_container])

    with kyuubi_context(kyuubi_context.on.leader_elected(), state) as manager:
        assert manager.charm.refresh is None
        assert manager.charm.refresh_ready_for_service_mesh() is False
