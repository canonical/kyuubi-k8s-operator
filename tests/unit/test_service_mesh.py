#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import json
from typing import TYPE_CHECKING
from unittest.mock import patch

from ops.testing import Container, Context, Relation, State

if TYPE_CHECKING:
    from charm import KyuubiCharm

SERVICE_MESH_RELATION = "service-mesh"
LABEL_CONFIGMAP_NAME = "juju-service-mesh-kyuubi-k8s-labels"


@patch("lightkube.Client")
@patch("charmlibs.interfaces.service_mesh._service_mesh.reconcile_charm_labels")
@patch("core.workload.kyuubi.KyuubiWorkload.exec")
def test_service_mesh_relation_adds_labels(
    exec_calls,
    reconcile_charm_labels,
    client_class,
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
