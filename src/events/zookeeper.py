
from charms.data_platform_libs.v0.data_interfaces import DatabaseRequirerEventHandlers
from constants import ZOOKEEPER_REL
from core.workload import KyuubiWorkloadBase
from events.base import BaseEventHandler, compute_status
from utils.logging import WithLogging
from managers.kyuubi import KyuubiManager
from ops import CharmBase
from core.context import Context


class ZookeeperEvents(BaseEventHandler, WithLogging):
    """Class implementing Zookeeper integration event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: KyuubiWorkloadBase):
        super().__init__(charm, "zookeeper")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kyuubi = KyuubiManager(self.workload)
        self.zookeeper_handler = DatabaseRequirerEventHandlers(self.charm, self.context.zookeeper_requirer_data)

        # self.framework.observe(self.charm.on[ZOOKEEPER_REL].relation_created, self._on_zookeeper_created)
        # self.framework.observe(self.charm.on[ZOOKEEPER_REL].relation_joined, self._on_zookeeper_joined)
        self.framework.observe(self.charm.on[ZOOKEEPER_REL].relation_changed, self._on_zookeeper_changed)
        self.framework.observe(self.charm.on[ZOOKEEPER_REL].relation_broken, self._on_zookeeper_broken)


    # def _on_zookeeper_created(self, _):
    #     self.logger.warning("Zookeeper created...")
    #     self.logger.warning(self.context.zookeeper.endpoints)


    # def _on_zookeeper_joined(self, _):
    #     self.logger.warning("Zookeeper joined...")

    @compute_status
    def _on_zookeeper_changed(self, _):
        self.logger.info("Zookeeper relation changed...")
        self.kyuubi.update(
            s3_info=self.context.s3,
            metastore_db_info=self.context.metastore_db,
            auth_db_info=self.context.auth_db,
            service_account_info=self.context.service_account,
            zookeeper_info=self.context.zookeeper
        )

    @compute_status
    def _on_zookeeper_broken(self, _):
        self.logger.info("Zookeeper relation broken...")
        self.kyuubi.update(
            s3_info=self.context.s3,
            metastore_db_info=self.context.metastore_db,
            auth_db_info=self.context.auth_db,
            service_account_info=self.context.service_account,
            zookeeper_info=None
        )
