"""Utility module containing logic of creation and deletion of managed Kyuubi K8s service."""

import enum
import functools
import socket
import typing

import lightkube
from ops import Model

from constants import JDBC_PORT, PEER_REL
from utils.logging import WithLogging


class _ServiceType(enum.Enum):
    """Supported K8s service types."""

    CLUSTER_IP = "ClusterIP"
    NODE_PORT = "NodePort"
    LOAD_BALANCER = "LoadBalancer"


class ServiceUtil(WithLogging):
    """Utility class containing logic of creation and deletion of managed Kyuubi K8s service."""

    def __init__(self, model: Model):
        self.model = model
        self.app_name = model.app.name
        self.service_name = f"{self.app_name}-service"
        self.namespace = model.name
        self.lightkube_agent = lightkube.Client()

    @property
    def model_service_domain(self) -> str:
        """K8s service domain for Juju model."""
        # Example: "kyuubi-k8s-0.kyuubi-k8s-endpoints.my-model.svc.cluster.local"
        fqdn = socket.getfqdn()
        # Example: "kyuubi-k8s-0.kyuubi-k8s-endpoints."
        prefix = f"{self.model.unit.name.replace('/', '-')}.{self.app_name}-endpoints."
        assert fqdn.startswith(f"{prefix}{self.namespace}.")
        # Example: my-model.svc.cluster.local
        return fqdn.removeprefix(prefix)

    @property
    def _host(self) -> str:
        """K8s service hostname for Kyuubi."""
        # Example: kyuubi-k8s-service.my-model.svc.cluster.local
        return f"{self.service_name}.{self.model_service_domain}"

    def _get_node_hosts(self) -> set[str]:
        """Return the node ports of nodes where units of this app are scheduled."""
        peer_relation = self.model.get_relation(PEER_REL)
        if not peer_relation:
            return []

        def _get_node_address(node) -> str:
            # OpenStack will return an internal hostname, not externally accessible
            # Preference: ExternalIP > InternalIP > Hostname
            for typ in ["ExternalIP", "InternalIP", "Hostname"]:
                for address in node.status.addresses:
                    if address.type == typ:
                        return address.address

        hosts = set()
        for unit in peer_relation.units | {self.model.unit}:
            node = self.get_node(unit.name)
            hosts.add(_get_node_address(node))
        return hosts

    @functools.cache
    def get_node(self, unit_name: str) -> lightkube.resources.core_v1.Node:
        """Return the node for the provided unit name."""
        node_name = self.get_pod(unit_name).spec.nodeName
        return self.lightkube_agent.get(
            res=lightkube.resources.core_v1.Node,
            name=node_name,
            namespace=self.namespace,
        )

    def get_service(self) -> typing.Optional[lightkube.resources.core_v1.Service]:
        """Get the managed k8s service."""
        try:
            service = self.lightkube_agent.get(
                res=lightkube.resources.core_v1.Service,
                name=self.service_name,
                namespace=self.namespace,
            )
        except lightkube.core.exceptions.ApiError as e:
            if e.status.code == 404:
                return None
            raise

        return service

    def get_jdbc_endpoints(self):
        """Return the list of JDBC endpoints to connect to Kyuubi."""
        service = self.get_service()
        if not service:
            return ""

        port = JDBC_PORT
        service_type = _ServiceType(service.spec.type)

        if service_type == _ServiceType.CLUSTER_IP:
            return f"{self._host}:{port}"
        elif service_type == _ServiceType.NODE_PORT:
            hosts = self._get_node_hosts()

            for p in service.spec.ports:
                node_port = p.nodePort
                break

            return ",".join(sorted({f"{host}:{node_port}" for host in hosts}))
        elif service_type == _ServiceType.LOAD_BALANCER and service.status.loadBalancer.ingress:
            hosts = set()
            for ingress in service.status.loadBalancer.ingress:
                hosts.add(ingress.ip)

            return ",".join(sorted(f"{host}:{port}" for host in hosts))

        return ""

    def delete_service(self):
        """Delete the existing managed K8s service."""
        try:
            self.lightkube_agent.delete(
                res=lightkube.resources.core_v1.Service,
                name=self.service_name,
                namespace=self.namespace,
            )
            self.logger.info("Deleted Kyuubi managed service.")
        except Exception as e:
            self.logger.warning("Cannot delete service %s, Reason: %s", self.service_name, str(e))

    @functools.cache
    def get_pod(self, unit_name: str) -> lightkube.resources.core_v1.Pod:
        """Get the pod for the provided unit name."""
        return self.lightkube_agent.get(
            res=lightkube.resources.core_v1.Pod,
            name=unit_name.replace("/", "-"),
            namespace=self.namespace,
        )

    def create_service(self, service_type, owner_references):
        """Create the Kubernetes service with desired service type."""
        desired_service = lightkube.resources.core_v1.Service(
            metadata=lightkube.models.meta_v1.ObjectMeta(
                name=self.service_name,
                namespace=self.namespace,
                ownerReferences=owner_references,  # the stateful set
                labels={"app.kubernetes.io/name": self.app_name},
            ),
            spec=lightkube.models.core_v1.ServiceSpec(
                ports=[
                    lightkube.models.core_v1.ServicePort(
                        name="kyuubi-jdbc",
                        port=JDBC_PORT,
                        targetPort=JDBC_PORT,
                    ),
                ],
                type=service_type.value,
                selector={"app.kubernetes.io/name": self.app_name},
            ),
        )
        try:
            self.lightkube_agent.apply(desired_service, field_manager=self.app_name)
            self.logger.info(f"Applied desired service  {service_type.value=}")
        except Exception as e:
            self.logger.warning("Cannot create service %s, Reason: %s", self.service_name, str(e))
            return False
        return True

    def reconcile_services(self, expose_external: str):
        """Update the services according to the desired service type."""
        desired_service_type = {
            "false": _ServiceType.CLUSTER_IP,
            "nodeport": _ServiceType.NODE_PORT,
            "loadbalancer": _ServiceType.LOAD_BALANCER,
        }[expose_external]

        existing_service = self.get_service()
        if existing_service is not None:
            if _ServiceType(existing_service.spec.type) == desired_service_type:
                self.logger.info(
                    f"Kyuubi is already exposed on a service of type {desired_service_type}."
                )
                return

            self.delete_service()

        pod0 = self.get_pod(f"{self.app_name}/0")

        self.create_service(
            service_type=desired_service_type, ownerReferences=pod0.metadata.ownerReferences
        )

    def is_service_connectable(self) -> bool:
        """Check whether the all endpoints are available for the connection."""
        if not self.get_service():
            self.logger.debug("No service exists yet.")
            return False

        endpoints = self.get_jdbc_endpoints()
        if endpoints == "":
            self.logger.debug("Empty Kyuubi service endpoints list")
            return False

        for endpoint in endpoints.split(","):
            with socket.socket() as s:
                host, port = endpoint.split(":")
                if s.connect_ex((host, int(port))) != 0:
                    self.logger.info(f"Unable to connect to {endpoint=}")
                    return False
        return True
