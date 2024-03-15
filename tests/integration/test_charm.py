#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import asyncio
import json
import logging
import subprocess
import urllib.request
from pathlib import Path
from time import sleep

import pytest
import yaml
from pytest_operator.plugin import OpsTest
# from tenacity import RetryError, Retrying, stop_after_attempt, wait_fixed

# from .test_helpers import fetch_action_sync_s3_credentials, setup_s3_bucket_for_history_server

logger = logging.getLogger(__name__)

APP_NAME = "kyuubi"
BUCKET_NAME = "kyuubi"
METADATA = yaml.safe_load(Path("./charmcraft.yaml").read_text())

@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, s3_bucket_and_creds):
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """

    # Receive S3 params from fixture
    endpoint_url = s3_bucket_and_creds["endpoint"]
    access_key = s3_bucket_and_creds["access_key"]
    secret_key = s3_bucket_and_creds["secret_key"]
    bucket_name = s3_bucket_and_creds["bucket"]

    # Build and deploy charm from local source folder
    logger.info("Building charm")
    charm = await ops_test.build_charm(".")

    image_version = METADATA["resources"]["spark-history-server-image"]["upstream-source"]

#     logger.info(f"Image version: {image_version}")

#     resources = {"spark-history-server-image": image_version}

#     logger.info("Deploying charm")

#     # Deploy the charm and wait for waiting status
#     await asyncio.gather(
#         ops_test.model.deploy(**charm_versions.s3.deploy_dict()),
#         ops_test.model.deploy(
#             charm, resources=resources, application_name=APP_NAME, num_units=1, series="jammy"
#         ),
#     )

#     await ops_test.model.wait_for_idle(
#         apps=[APP_NAME, charm_versions.s3.application_name], timeout=1000
#     )

#     s3_integrator_unit = ops_test.model.applications[charm_versions.s3.application_name].units[0]

#     logger.info("Setting up s3 credentials in s3-integrator charm")

#     await fetch_action_sync_s3_credentials(
#         s3_integrator_unit, access_key=access_key, secret_key=secret_key
#     )
#     async with ops_test.fast_forward():
#         await ops_test.model.wait_for_idle(
#             apps=[charm_versions.s3.application_name], status="active"
#         )

#     configuration_parameters = {
#         "bucket": "history-server",
#         "path": "spark-events",
#         "endpoint": endpoint_url,
#     }
#     # apply new configuration options
#     await ops_test.model.applications[charm_versions.s3.application_name].set_config(
#         configuration_parameters
#     )

#     logger.info("Relating history server charm with s3-integrator charm")

#     await ops_test.model.add_relation(charm_versions.s3.application_name, APP_NAME)

#     await ops_test.model.wait_for_idle(
#         apps=[APP_NAME, charm_versions.s3.application_name], timeout=1000
#     )

#     # wait for active status
#     await ops_test.model.wait_for_idle(
#         apps=[APP_NAME],
#         status="active",
#         timeout=1000,
#     )

#     logger.info("Verifying history server has no app entries")

#     status = await ops_test.model.get_status()
#     address = status["applications"][APP_NAME]["units"][f"{APP_NAME}/0"]["address"]

#     apps = json.loads(urllib.request.urlopen(f"http://{address}:18080/api/v1/applications").read())

#     assert len(apps) == 0

#     logger.info("Setting up spark")

#     setup_spark_output = subprocess.check_output(
#         f"./tests/integration/setup/setup_spark.sh {endpoint_url} {access_key} {secret_key}",
#         shell=True,
#         stderr=None,
#     ).decode("utf-8")

#     logger.info(f"Setup spark output:\n{setup_spark_output}")

#     logger.info("Executing Spark job")

#     run_spark_output = subprocess.check_output(
#         "./tests/integration/setup/run_spark_job.sh", shell=True, stderr=None
#     ).decode("utf-8")

#     logger.info(f"Run spark output:\n{run_spark_output}")

#     logger.info("Verifying history server has 1 app entry")

#     for i in range(0, 5):
#         try:
#             apps = json.loads(
#                 urllib.request.urlopen(f"http://{address}:18080/api/v1/applications").read()
#             )
#         except Exception:
#             apps = []

#         if len(apps) > 0:
#             break
#         else:
#             sleep(3)

#     assert len(apps) == 1


# @pytest.mark.abort_on_fail
# async def test_ingress(ops_test: OpsTest, charm_versions):
#     """Build the charm-under-test and deploy it together with related charms.

#     Assert on the unit status before any relations/configurations take place.
#     """
#     # Deploy the charm and wait for waiting status
#     _ = await ops_test.model.deploy(**charm_versions.ingress.deploy_dict())

#     await ops_test.model.wait_for_idle(
#         apps=[charm_versions.ingress.application_name],
#         status="active",
#         timeout=300,
#         idle_period=30,
#     )

#     logger.info("Relating history server charm with ingress")

#     await ops_test.model.add_relation(charm_versions.ingress.application_name, APP_NAME)

#     await ops_test.model.wait_for_idle(
#         apps=[APP_NAME, charm_versions.ingress.application_name],
#         status="active",
#         timeout=300,
#         idle_period=30,
#     )

#     action = await ops_test.model.units.get(
#         f"{charm_versions.ingress.application_name}/0"
#     ).run_action(
#         "show-proxied-endpoints",
#     )

#     ingress_endpoint = json.loads((await action.wait()).results["proxied-endpoints"])[APP_NAME][
#         "url"
#     ]

#     logger.info(f"Querying endpoint: {ingress_endpoint}/api/v1/applications")

#     apps = json.loads(urllib.request.urlopen(f"{ingress_endpoint}/api/v1/applications").read())

#     assert len(apps) == 1

#     logger.info(f"Number of apps: {len(apps)}")


# @pytest.mark.abort_on_fail
# async def test_oathkeeper(ops_test: OpsTest, charm_versions):
#     """Test the integration of the spark history server with Oathkeeper.

#     Assert that the proxied-enpoints of the ingress are protected (err code 401).
#     """
#     # remove relation between ingress and spark-history server
#     await ops_test.model.applications[APP_NAME].remove_relation(
#         f"{APP_NAME}:ingress", f"{charm_versions.ingress.application_name}:ingress"
#     )

#     await ops_test.model.wait_for_idle(
#         apps=[APP_NAME, charm_versions.ingress.application_name],
#         status="active",
#         timeout=300,
#         idle_period=30,
#     )

#     # Deploy the oathkeeper charm and wait for waiting status
#     _ = await ops_test.model.deploy(**charm_versions.oathkeeper.deploy_dict(), trust=True)

#     await ops_test.model.wait_for_idle(
#         apps=[charm_versions.oathkeeper.application_name],
#         status="active",
#         timeout=300,
#         idle_period=30,
#     )

#     # configure Oathkeeper charm
#     oathkeeper_configuration_parameters = {"dev": "True"}
#     await ops_test.model.applications[charm_versions.oathkeeper.application_name].set_config(
#         oathkeeper_configuration_parameters
#     )

#     await ops_test.model.wait_for_idle(
#         apps=[charm_versions.oathkeeper.application_name],
#         status="active",
#         timeout=300,
#         idle_period=30,
#     )
#     # configure ingress to work with Oathkeeper
#     ingress_configuration_parameters = {"enable_experimental_forward_auth": "True"}
#     # apply new configuration options
#     await ops_test.model.applications[charm_versions.ingress.application_name].set_config(
#         ingress_configuration_parameters
#     )

#     await ops_test.model.wait_for_idle(
#         apps=[charm_versions.ingress.application_name],
#         status="active",
#         timeout=300,
#         idle_period=30,
#     )
#     # Relate Oathkeeper with the Spark history server charm
#     logger.info("Relating the spark history server charm with oathkeeper.")
#     await ops_test.model.add_relation(charm_versions.oathkeeper.application_name, APP_NAME)

#     await ops_test.model.wait_for_idle(
#         apps=[APP_NAME],
#         status="blocked",
#         timeout=300,
#         idle_period=30,
#     )
#     # relate spark-history-server and ingress
#     await ops_test.model.add_relation(charm_versions.ingress.application_name, APP_NAME)

#     await ops_test.model.wait_for_idle(
#         apps=[APP_NAME, charm_versions.ingress.application_name],
#         status="active",
#         timeout=300,
#         idle_period=30,
#     )

#     # Relate Oathkeeper with the Ingress charm
#     logger.info("Relating the oathkeeper charm with the ingress.")
#     await ops_test.model.add_relation(
#         f"{charm_versions.ingress.application_name}:experimental-forward-auth",
#         charm_versions.oathkeeper.application_name,
#     )

#     await ops_test.model.wait_for_idle(
#         apps=[charm_versions.oathkeeper.application_name, charm_versions.ingress.application_name],
#         status="active",
#         timeout=300,
#         idle_period=30,
#     )

#     # get proxied endpoint
#     action = await ops_test.model.units.get(
#         f"{charm_versions.ingress.application_name}/0"
#     ).run_action(
#         "show-proxied-endpoints",
#     )

#     ingress_endpoint = json.loads((await action.wait()).results["proxied-endpoints"])[APP_NAME][
#         "url"
#     ]

#     # check that the ingress endpoint is not authorized!
#     logger.info(f"Querying endpoint: {ingress_endpoint}")
#     try:
#         _ = urllib.request.urlopen(ingress_endpoint)
#         raise Exception(
#             "Successful request.... something is wrong with the protection of the endpoints."
#         )
#     except urllib.error.HTTPError as e:  # type: ignore
#         # Return code error (e.g. 404, 501, ...)
#         logger.info("HTTPError: {}".format(e.code))
#         # check that the endopoint respond with code 401
#         assert e.code == 401

#     logger.info(f"Endpoint: {ingress_endpoint} successfully protected.")


# @pytest.mark.skip
# async def test_remove_oathkeeper(ops_test: OpsTest, charm_versions):
#     """Test the removal of integration between the spark history server and Oathkeeper.

#     Assert that the proxied-enpoints of the ingress are not protected.
#     """
#     # Remove of the relation between oathkeeper and spark-history server
#     await ops_test.model.applications[APP_NAME].remove_relation(
#         f"{APP_NAME}:auth-proxy", f"{charm_versions.oathkeeper.application_name}:auth-proxy"
#     )

#     await ops_test.model.wait_for_idle(
#         apps=[APP_NAME, charm_versions.oathkeeper.application_name],
#         status="active",
#         timeout=300,
#         idle_period=30,
#     )

#     try:
#         for attempt in Retrying(stop=stop_after_attempt(10), wait=wait_fixed(30)):
#             with attempt:
#                 action = await ops_test.model.units.get(
#                     f"{charm_versions.ingress.application_name}/0"
#                 ).run_action(
#                     "show-proxied-endpoints",
#                 )

#                 ingress_endpoint = json.loads((await action.wait()).results["proxied-endpoints"])[
#                     APP_NAME
#                 ]["url"]

#                 logger.info(f"Trying to querying endpoint: {ingress_endpoint}/api/v1/applications")

#                 apps = json.loads(
#                     urllib.request.urlopen(f"{ingress_endpoint}/api/v1/applications").read()
#                 )

#                 assert len(apps) == 1

#                 logger.info(f"Number of apps: {len(apps)}")
#     except RetryError:
#         raise Exception("Failed to reach the endpoint!")
