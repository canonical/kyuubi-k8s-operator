# Copyright 2024 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

r"""A library for creating service accounts that are configured to run Spark jobs."""
import json
import logging
from collections import namedtuple
from typing import List, Optional, Union

from charms.data_platform_libs.v0.data_interfaces import (
    SECRET_GROUPS,
    EventHandlers,
    ProviderData,
    RequirerData,
    RequirerEventHandlers,
)
from ops import Model, RelationCreatedEvent, RelationDepartedEvent, SecretChangedEvent
from ops.charm import (
    CharmBase,
    CharmEvents,
    RelationBrokenEvent,
    RelationChangedEvent,
    RelationEvent,
)
from ops.framework import EventSource, ObjectEvents
from ops.model import Application, Unit

# The unique Charmhub library identifier, never change it
LIBID = ""

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1


logger = logging.getLogger(__name__)

Diff = namedtuple("Diff", "added changed deleted")
Diff.__doc__ = """
A tuple for storing the diff between two data mappings.

added - keys that were added
changed - keys that still exist but have new values
deleted - key that were deleted"""


def diff(event: RelationChangedEvent, bucket: Union[Unit, Application]) -> Diff:
    """Retrieves the diff of the data in the relation changed databag.

    Args:
        event: relation changed event.
        bucket: bucket of the databag (app or unit)

    Returns:
        a Diff instance containing the added, deleted and changed
            keys from the event relation databag.
    """
    # Retrieve the old data from the data key in the application relation databag.
    old_data = json.loads(event.relation.data[bucket].get("data", "{}"))
    # Retrieve the new data from the event relation databag.
    new_data = (
        {key: value for key, value in event.relation.data[event.app].items() if key != "data"}
        if event.app
        else {}
    )

    # These are the keys that were added to the databag and triggered this event.
    added = new_data.keys() - old_data.keys()
    # These are the keys that were removed from the databag and triggered this event.
    deleted = old_data.keys() - new_data.keys()
    # These are the keys that already existed in the databag,
    # but had their values changed.
    changed = {key for key in old_data.keys() & new_data.keys() if old_data[key] != new_data[key]}

    # TODO: evaluate the possibility of losing the diff if some error
    # happens in the charm before the diff is completely checked (DPE-412).
    # Convert the new_data to a serializable format and save it for a next diff check.
    event.relation.data[bucket].update({"data": json.dumps(new_data)})

    # Return the diff with all possible changes.
    return Diff(added, changed, deleted)


class ServiceAccountEvent(RelationEvent):
    """Base class for Service account events."""

    @property
    def service_account(self) -> Optional[str]:
        """Returns the service account was requested."""
        if not self.relation.app:
            return None

        return self.relation.data[self.relation.app].get("service-account", "")

    @property
    def namespace(self) -> Optional[str]:
        """Returns the namespace that was requested."""
        if not self.relation.app:
            return None

        return self.relation.data[self.relation.app].get("namespace", "")


class ServiceAccountRequestedEvent(ServiceAccountEvent):
    """Event emitted when a set of service account/namespace is requested for use on this relation."""


class ServiceAccountReleasedEvent(ServiceAccountEvent):
    """Event emitted when a set of service account/namespace is released."""


class IntegrationHubProviderEvents(CharmEvents):
    """Event descriptor for events raised by ServiceAccountProvider."""

    account_requested = EventSource(ServiceAccountRequestedEvent)
    account_released = EventSource(ServiceAccountReleasedEvent)


class ServiceAccountGrantedEvent(ServiceAccountEvent):
    """Event emitted when service account are granted on this relation."""


class ServiceAccountGoneEvent(RelationEvent):
    """Event emitted when service account are removed from this relation."""


class IntegrationHubRequirerEvents(ObjectEvents):
    """Event descriptor for events raised by the Requirer."""

    account_granted = EventSource(ServiceAccountGrantedEvent)
    account_gone = EventSource(ServiceAccountGoneEvent)


# Integration Hub Provider and Requirer


class IntegrationHubProviderData(ProviderData):
    """Provider-side of the Spark Integration Hub relation."""

    def __init__(self, model: Model, relation_name: str) -> None:
        super().__init__(model, relation_name)

    def set_service_account(self, relation_id: int, service_account: str) -> None:
        """Set the service account name in the application relation databag.

        Args:
            relation_id: the identifier for a particular relation.
            service_account: the service account name.
        """
        self.update_relation_data(relation_id, {"service-account": service_account})

    def set_namespace(self, relation_id: int, namespace: str) -> None:
        """Set the bootstrap server in the application relation databag.

        Args:
            relation_id: the identifier for a particular relation.
            namespace: the namespace name.
        """
        self.update_relation_data(relation_id, {"namespace": namespace})


class IntegrationHubProviderEventHandlers(EventHandlers):
    """Provider-side of the Integration Hub relation."""

    on = IntegrationHubProviderEvents()  # pyright: ignore [reportAssignmentType]

    def __init__(self, charm: CharmBase, relation_data: IntegrationHubProviderData) -> None:
        super().__init__(charm, relation_data)
        # Just to keep lint quiet, can't resolve inheritance. The same happened in super().__init__() above
        self.relation_data = relation_data
        self.framework.observe(
            charm.on[self.relation_data.relation_name].relation_departed,
            self._on_relation_departed,
        )

    def _on_relation_changed_event(self, event: RelationChangedEvent) -> None:
        """Event emitted when the relation has changed."""
        # Leader only
        if not self.relation_data.local_unit.is_leader():
            return

        diff = self._diff(event)
        # emit on account requested if service account name is provided by the requirer application
        if "service-account" in diff.added and "namespace" in diff.added:
            getattr(self.on, "account_requested").emit(
                event.relation, app=event.app, unit=event.unit
            )

    def _on_relation_departed(self, event: RelationDepartedEvent) -> None:
        """React to the relation changed event by consuming data."""
        # Leader only
        if not self.relation_data.local_unit.is_leader():
            return

        getattr(self.on, "account_released").emit(event.relation, app=event.app, unit=event.unit)


class IntegrationHubProvider(IntegrationHubProviderData, IntegrationHubProviderEventHandlers):
    """Provider-side of the Integration Hub relation."""

    def __init__(self, charm: CharmBase, relation_name: str) -> None:
        IntegrationHubProviderData.__init__(self, charm.model, relation_name)
        IntegrationHubProviderEventHandlers.__init__(self, charm, self)


class IntegrationHubRequirerData(RequirerData):
    """Requirer-side of the Integration Hub relation."""

    def __init__(
        self,
        model: Model,
        relation_name: str,
        service_account: str,
        namespace: str,
        additional_secret_fields: Optional[List[str]] = [],
    ):
        """Manager of Integration Hub relations."""
        super().__init__(model, relation_name, additional_secret_fields=additional_secret_fields)
        self.service_account = service_account
        self.namespace = namespace

    @property
    def service_account(self):
        """Service account used for Spark."""
        return self._service_account

    @service_account.setter
    def service_account(self, value):
        self._service_account = value

    @property
    def namespace(self):
        """Namespace used for running Spark jobs."""
        return self._namespace

    @namespace.setter
    def namespace(self, value):
        self._namespace = value


class IntegrationHubRequirerEventHandlers(RequirerEventHandlers):
    """Requirer-side of the Integration Hub relation."""

    on = IntegrationHubRequirerEvents()  # pyright: ignore [reportAssignmentType]

    def __init__(self, charm: CharmBase, relation_data: IntegrationHubRequirerData) -> None:
        super().__init__(charm, relation_data)
        # Just to keep lint quiet, can't resolve inheritance. The same happened in super().__init__() above
        self.relation_data = relation_data
        self.framework.observe(
            charm.on[self.relation_data.relation_name].relation_broken,
            self._on_relation_broken,
        )

    def _on_relation_created_event(self, event: RelationCreatedEvent) -> None:
        """Event emitted when the Integration Hub relation is created."""
        super()._on_relation_created_event(event)

        if not self.relation_data.local_unit.is_leader():
            return

        # Sets service_account, namespace in the relation
        relation_data = {
            f: getattr(self.relation_data, f.replace("-", "_"), "")
            for f in ["service-account", "namespace"]
        }

        self.relation_data.update_relation_data(event.relation.id, relation_data)

    def _on_secret_changed_event(self, _: SecretChangedEvent):
        """Event notifying about a new value of a secret."""
        pass

    def _on_relation_changed_event(self, event: RelationChangedEvent) -> None:
        """Event emitted when the Integration Hub relation has changed."""
        logger.info("On Integration Hub relation changed")
        # Check which data has changed to emit customs events.
        diff = self._diff(event)

        # Check if the service-account is created in the desired namespace

        # Register all new secrets with their labels
        if any(newval for newval in diff.added if self.relation_data._is_secret_field(newval)):
            self.relation_data._register_secrets_to_relation(event.relation, diff.added)

        secret_field_user = self.relation_data._generate_secret_field_name(SECRET_GROUPS.USER)

        if (
            "service-account" in diff.added and "namespace" in diff.added
        ) or secret_field_user in diff.added:
            getattr(self.on, "account_granted").emit(
                event.relation, app=event.app, unit=event.unit
            )

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Notify the charm about a broken service account relation."""
        logger.info("On Integration Hub relation gone")
        getattr(self.on, "account_gone").emit(event.relation, app=event.app, unit=event.unit)


class IntegrationHubRequirer(IntegrationHubRequirerData, IntegrationHubRequirerEventHandlers):
    """Provider-side of the Integration Hub relation."""

    def __init__(
        self,
        charm: CharmBase,
        relation_name: str,
        service_account: str,
        namespace: str,
        additional_secret_fields: Optional[List[str]] = [],
    ) -> None:
        IntegrationHubRequirerData.__init__(
            self,
            charm.model,
            relation_name,
            service_account,
            namespace,
            additional_secret_fields,
        )
        IntegrationHubRequirerEventHandlers.__init__(self, charm, self)
