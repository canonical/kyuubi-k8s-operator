"""Common domain objects."""

from collections.abc import MutableMapping

from charms.data_platform_libs.v0.data_interfaces import Data
from ops import Application, Relation, Unit


class RelationState:
    """Relation state object."""

    def __init__(
        self, relation: Relation | None, data_interface: Data, component: Unit | Application | None
    ):
        """Initialize class for relation data."""
        self.relation = relation
        self.data_interface = data_interface
        self.component = (
            component  # FIXME: remove, and use _fetch_my_relation_data defaults wheren needed
        )
        self.relation_data = (
            self.data_interface.as_dict(self.relation.id) if self.relation else {}
        )  # FIXME: mappingproxytype?

    def __bool__(self) -> bool:
        """Boolean evaluation based on the existence of self.relation."""
        try:
            return bool(self.relation)
        except AttributeError:
            return False

    @property
    def data(self) -> MutableMapping:
        """Data representing the state."""
        return self.relation_data

    def update(self, items: dict[str, str]) -> None:
        """Writes to relation_data."""
        delete_fields = [key for key in items if not items[key]]
        update_content = {k: items[k] for k in items if k not in delete_fields}

        self.relation_data.update(update_content)

        for field in delete_fields:
            del self.relation_data[field]
