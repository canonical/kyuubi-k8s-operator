# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from __future__ import annotations

import contextlib
import logging
import shutil
import uuid
import zipfile
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Generator

import jubilant
import tomli
import tomli_w
import yaml
from spark8t.utils import PropertyFile

from core.domain import Status
from core.enums import ExposeExternal

from ..types import IntegrationTestsCharms, S3Info
from .auth import setup_jdbc_authentication, setup_ldap_authentication
from .k8s import run_command_in_pod

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

LATEST_STABLE_REV = 112


def get_random_name():
    return str(uuid.uuid4()).replace("-", "_")


def deploy_minimal_kyuubi_setup(
    juju: jubilant.Juju,
    kyuubi_charm: str | Path,
    charm_versions: IntegrationTestsCharms,
    s3_bucket_and_creds: S3Info,
    trust: bool = True,
    num_units=1,
    integrate_zookeeper=False,
    deploy_from_charmhub=False,
    integrate_data_integrator=True,
    auth_mode: str = "jdbc",
    expose_external: ExposeExternal = ExposeExternal.FALSE,
) -> None:
    deploy_args = {
        "app": APP_NAME,
        "num_units": num_units,
        "channel": "3.5/edge",
        "base": "ubuntu@22.04",
        "trust": trust,
        "revision": LATEST_STABLE_REV,
    }
    if not deploy_from_charmhub:
        image_version = METADATA["resources"]["kyuubi-image"]["upstream-source"]
        resources = {"kyuubi-image": image_version}
        logger.info(f"Image version: {image_version}")

        deploy_args.update({"resources": resources})

    logger.info("Deploying kyuubi-k8s charm...")
    juju.deploy(kyuubi_charm, **deploy_args)

    logger.info("Waiting for kyuubi-k8s app to settle...")
    status = juju.wait(jubilant.all_blocked)

    logger.info("Configuring kyuubi-k8s charm...")
    namespace = juju.model
    username = "kyuubi-spark-engine"
    charm_config = {
        "namespace": namespace,
        "service-account": username,
        "expose-external": expose_external.value,
    }
    juju.config(APP_NAME, charm_config)

    # try to apply profile = testing
    try:
        juju.config(APP_NAME, {"profile": "testing"})
    except Exception:
        # the previous version of the charm (for example in the refresh tests) may not have the profile config option.
        logger.info("Application of profile config option failed.")

    logger.info("Waiting for kyuubi-k8s app to settle...")
    status = juju.wait(jubilant.all_blocked)
    assert status.apps[APP_NAME].app_status.message == Status.MISSING_INTEGRATION_HUB.value.message

    logger.info("Deploying mandatory charms...")
    juju.deploy(**charm_versions.s3.deploy_dict())
    juju.deploy(**charm_versions.integration_hub.deploy_dict())

    logger.info("Waiting for s3-integrator app to be idle...")
    status = juju.wait(
        lambda status: jubilant.all_blocked(status, charm_versions.s3.app),
    )

    logger.info("Configuring s3-integrator...")
    endpoint_url = s3_bucket_and_creds["endpoint"]
    access_key = s3_bucket_and_creds["access_key"]
    secret_key = s3_bucket_and_creds["secret_key"]
    bucket_name = s3_bucket_and_creds["bucket"]
    path = s3_bucket_and_creds["path"]
    region = s3_bucket_and_creds["region"]
    tls_ca_chain = s3_bucket_and_creds["tls_ca_chain"]
    logger.info("Setting up s3 credentials in s3-integrator charm")
    task = juju.run(
        f"{charm_versions.s3.app}/0",
        "sync-s3-credentials",
        {"access-key": access_key, "secret-key": secret_key},
    )
    assert task.return_code == 0
    logger.info("Setting configuration for s3-integrator charm...")
    juju.config(
        charm_versions.s3.app,
        {
            "bucket": bucket_name,
            "path": path,
            "region": region,
            "endpoint": endpoint_url,
            "tls-ca-chain": tls_ca_chain,
        },
    )
    logger.info("Waiting for s3-integrator app to be idle and active...")
    juju.wait(lambda status: jubilant.all_active(status, charm_versions.s3.app))

    logger.info("Waiting for integration_hub and s3-integrator app to be idle and active...")
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.s3.app,
            charm_versions.integration_hub.app,
        )
    )

    logger.info("Integrating integration-hub charm with s3-integrator charm...")
    juju.integrate(charm_versions.s3.app, charm_versions.integration_hub.app)

    logger.info("Waiting for s3-integrator and integration-hub charms to be idle and active...")
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.s3.app,
            charm_versions.integration_hub.app,
        ),
        delay=15,
    )

    logger.info("Integrating kyuubi charm with integration-hub charm...")
    juju.integrate(charm_versions.integration_hub.app, APP_NAME)

    logger.info("Waiting for s3-integrator and integration_hub charms to be idle and active...")
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.s3.app,
            charm_versions.integration_hub.app,
        ),
        delay=15,
    )

    if auth_mode == "jdbc":
        setup_jdbc_authentication(juju=juju, charm_versions=charm_versions)
    elif auth_mode == "ldap":
        setup_ldap_authentication(juju=juju, charm_versions=charm_versions)

    if integrate_zookeeper:
        # Deploy Zookeeper and wait
        juju.deploy(**charm_versions.zookeeper.deploy_dict())
        logger.info("Waiting for zookeeper-k8s charm to be active and idle...")
        juju.wait(
            lambda status: jubilant.all_active(
                status,
                charm_versions.zookeeper.app,
            ),
        )

        # Integrate Kyuubi with Zookeeper and wait
        logger.info("Integrating kyuubi charm with zookeeper charm...")
        juju.integrate(charm_versions.zookeeper.app, APP_NAME)
        logger.info(
            "Waiting for s3-integrator, integration_hub and zookeeper to be idle and active..."
        )
        juju.wait(
            lambda status: jubilant.all_active(
                status,
                charm_versions.s3.app,
                charm_versions.integration_hub.app,
                charm_versions.zookeeper.app,
            ),
            delay=5,
        )

    if integrate_data_integrator:
        juju.deploy(
            **charm_versions.data_integrator.deploy_dict(), config={"database-name": "test"}
        )
        logger.info("Waiting for data-integrator charm to be idle...")
        juju.wait(lambda status: jubilant.all_blocked(status, charm_versions.data_integrator.app))
        logger.info("Integrating kyuubi charm with data-integrator charm...")
        juju.integrate(charm_versions.data_integrator.app, APP_NAME)
        juju.wait(jubilant.all_active, delay=3)

    logger.info("Successfully deployed minimal working Kyuubi setup.")


def fetch_spark_properties(juju: jubilant.Juju, unit_name: str) -> dict[str, str]:
    pod_name = unit_name.replace("/", "-")
    command = ["cat", "/etc/spark8t/conf/spark-defaults.conf"]
    stdout, _ = run_command_in_pod(juju, pod_name=pod_name, pod_command=command)
    with NamedTemporaryFile(mode="w+") as temp_file:
        temp_file.write(stdout)
        temp_file.seek(0)
        props = PropertyFile.read(temp_file.name).props
        return props


@contextlib.contextmanager
def inject_dependency_fault(original_charm_file: Path) -> Generator[Path, None, None]:
    """Inject a dependency fault into the Kyuubi charm."""
    filename = Path(original_charm_file).name
    tmp = Path("tmp")
    tmp.mkdir(exist_ok=True)
    fault_charm = tmp / filename
    shutil.copy(original_charm_file, fault_charm)

    logger.info("Inject dependency fault")
    with Path("refresh_versions.toml").open("rb") as file:
        versions = tomli.load(file)

    versions["charm"] = "1/0.0.0"  # Let's use a track that does not exist

    # Overwrite refresh_versions.toml with incompatible version.
    with zipfile.ZipFile(fault_charm, mode="a") as charm_zip:
        charm_zip.writestr("refresh_versions.toml", tomli_w.dumps(versions))

    yield fault_charm

    fault_charm.unlink(missing_ok=True)
    tmp.rmdir()
