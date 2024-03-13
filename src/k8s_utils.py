from lightkube import Client
from lightkube.resources.core_v1 import Namespace, ServiceAccount
from lightkube.core.exceptions import ApiError

def is_valid_namespace(namespace: str) -> bool:
    try:
        Client().get(Namespace, name=namespace)
    except ApiError:
        return False
    return True


def is_valid_service_account(namespace: str, service_account: str) -> bool:
    try:
        Client().get(ServiceAccount, name=service_account, namespace=namespace)
    except ApiError:
        return False
    return True
