"""Utility module containing logic of creation and deletion of managed Kyuubi K8s service."""

import enum
import functools
import socket

import lightkube
from lightkube.core.exceptions import ApiError
from lightkube.models.core_v1 import LoadBalancerIngress, ServicePort, ServiceSpec
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Node, Pod, Service

from constants import JDBC_PORT
from utils.logging import WithLogging


class LbHost(str):
    """Enable runtime check on loadbalancer ingress type."""

    pass


class _ServiceType(enum.Enum):
    """Supported K8s service types."""

    CLUSTER_IP = "ClusterIP"
    NODE_PORT = "NodePort"
    LOAD_BALANCER = "LoadBalancer"


class ServiceManager(WithLogging):
    """Utility class containing logic of creation and deletion of managed Kyuubi K8s service."""

    def __init__(self, namespace: str, unit_name: str, app_name: str):
        self.namespace = namespace
        self.unit_name = unit_name
        self.app_name = app_name
        self.service_name = f"{self.app_name}-service"
        self.lightkube_agent = lightkube.Client()

    @property
    def model_service_domain(self) -> str:
        """K8s service domain for Juju model."""
        # Example: "kyuubi-k8s-0.kyuubi-k8s-endpoints.my-model.svc.cluster.local"
        fqdn = socket.getfqdn()
        # Example: "kyuubi-k8s-0.kyuubi-k8s-endpoints."
        prefix = f"{self.unit_name.replace('/', '-')}.{self.app_name}-endpoints."
        assert fqdn.startswith(f"{prefix}{self.namespace}.")
        # Example: my-model.svc.cluster.local
        return fqdn.removeprefix(prefix)

    @property
    def _host(self) -> str:
        """K8s service hostname for Kyuubi."""
        # Example: kyuubi-k8s-service.my-model.svc.cluster.local
        return f"{self.service_name}.{self.model_service_domain}"

    def _get_node_host(self) -> set[str]:
        """Return the node ports of nodes where units of this app are scheduled."""

        def _get_node_address(node) -> str:
            # OpenStack will return an internal hostname, not externally accessible
            # Preference: ExternalIP > InternalIP > Hostname
            for typ in ["ExternalIP", "InternalIP", "Hostname"]:
                for address in node.status.addresses:
                    if address.type == typ:
                        return address.address

        node = self.get_node(self.unit_name)
        host = _get_node_address(node)
        return host

    @functools.cache
    def get_node(self, unit_name: str) -> Node:
        """Return the node for the provided unit name."""
        node_name = self.get_pod(unit_name).spec.nodeName
        return self.lightkube_agent.get(
            res=Node,
            name=node_name,
            namespace=self.namespace,
        )

    def get_service(self) -> Service | None:
        """Get the managed k8s service."""
        try:
            service = self.lightkube_agent.get(
                res=Service,
                name=self.service_name,
                namespace=self.namespace,
            )
        except ApiError as e:
            if e.status.code == 404:
                return None
            raise

        return service

    def get_service_endpoint(self, expose_external: str) -> str:
        """Returns the endpoint that can be used to connect to the service."""
        service = self.get_service()
        expected_service_type = {
            "false": _ServiceType.CLUSTER_IP,
            "nodeport": _ServiceType.NODE_PORT,
            "loadbalancer": _ServiceType.LOAD_BALANCER,
        }[expose_external]

        match service:
            case Service(spec=ServiceSpec(type)) if type != expected_service_type:
                return ""
            case Service(spec=ServiceSpec(type=_ServiceType.CLUSTER_IP)):
                return f"{self._host}:{JDBC_PORT}"

            case Service(spec=ServiceSpec(type=_ServiceType.NODE_PORT, ports=[*ports])):
                host = self._get_node_host()
                for p in ports:
                    node_port = p.nodePort
                    break
                return f"{host}:{node_port}"

            case Service(spec=ServiceSpec(type=_ServiceType.LOAD_BALANCER)):
                if not service.status.loadbalancer.ingress:
                    return ""

                lb: LoadBalancerIngress
                for lb in service.status.loadBalancer.ingress:
                    if lb.ip is not None:
                        return f"{lb.ip}:{JDBC_PORT}"
                    elif lb.hostname is not None:
                        return LbHost(f"{lb.hostname}:{JDBC_PORT}")
                raise Exception(f"Unable to find LoadBalancer ingress for the {service} service")

            case _:
                # Covers k8s connectivity error, mismatched services
                return ""

    def delete_service(self):
        """Delete the existing managed K8s service."""
        try:
            self.lightkube_agent.delete(
                res=Service,
                name=self.service_name,
                namespace=self.namespace,
            )
            self.logger.info("Deleted Kyuubi managed service.")
        except Exception as e:
            self.logger.warning("Cannot delete service %s, Reason: %s", self.service_name, str(e))

    @functools.cache
    def get_pod(self, unit_name: str) -> Pod:
        """Get the pod for the provided unit name."""
        return self.lightkube_agent.get(
            res=Pod,
            name=unit_name.replace("/", "-"),
            namespace=self.namespace,
        )

    def create_service(self, service_type, owner_references):
        """Create the Kubernetes service with desired service type."""
        desired_service = Service(
            metadata=ObjectMeta(
                name=self.service_name,
                namespace=self.namespace,
                ownerReferences=owner_references,  # the stateful set
                labels={"app.kubernetes.io/name": self.app_name},
            ),
            spec=ServiceSpec(
                ports=[
                    ServicePort(
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

            # self.delete_service()

        pod0 = self.get_pod(f"{self.app_name}/0")

        self.create_service(
            service_type=desired_service_type, owner_references=pod0.metadata.ownerReferences
        )

    def get_node_ip(self, pod_name: str) -> str:
        """Gets the IP Address of the Node of a given Pod via the K8s API."""
        try:
            node = self.get_node(pod_name)
        except ApiError as e:
            if e.status.code == 403:
                return ""
        if not node.status or not node.status.addresses:
            raise Exception(f"No status found for {node}")
        for addresses in node.status.addresses:
            if addresses.type in ["ExternalIP", "InternalIP", "Hostname"]:
                return addresses.address
        return ""
