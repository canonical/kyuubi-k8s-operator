#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
import subprocess
import uuid
from pathlib import Path

import psycopg2
import pytest
import yaml
from pytest_operator.plugin import OpsTest

from constants import (
    AUTHENTICATION_DATABASE_NAME,
    KYUUBI_CLIENT_RELATION_NAME,
    METASTORE_DATABASE_NAME,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
TEST_CHARM_PATH = "./tests/integration/app-charm"
TEST_CHARM_NAME = "application"


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, service_account):
    """Test building and deploying the charm without relation with any other charm."""
    # Build and deploy charm from local source folder
    logger.info("Building charm...")
    charm = await ops_test.build_charm(".")

    image_version = METADATA["resources"]["kyuubi-image"]["upstream-source"]
    resources = {"kyuubi-image": image_version}
    logger.info(f"Image version: {image_version}")

    # Deploy the charm and wait for waiting status
    logger.info("Deploying kyuubi-k8s charm...")
    await ops_test.model.deploy(
        charm,
        resources=resources,
        application_name=APP_NAME,
        num_units=1,
        series="jammy",
        trust=True,
    )

    logger.info("Waiting for kyuubi-k8s app to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        timeout=1000,
    )
    logger.info(f"State of kyuubi-k8s app: {ops_test.model.applications[APP_NAME].status}")

    logger.info("Setting configuration for kyuubi-k8s charm...")
    namespace, username = service_account
    await ops_test.model.applications[APP_NAME].set_config(
        {"namespace": namespace, "service-account": username}
    )

    logger.info("Waiting for kyuubi-k8s app to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="blocked",
        timeout=1000,
    )

    # Assert that the charm is in blocked state, waiting for S3 relation
    assert ops_test.model.applications[APP_NAME].status == "blocked"


@pytest.mark.abort_on_fail
async def test_integration_with_s3_integrator(
    ops_test: OpsTest, charm_versions, s3_bucket_and_creds
):
    """Test the charm by integrating it with s3-integrator."""
    # Deploy the charm and wait for waiting status
    logger.info("Deploying s3-integrator charm...")
    await ops_test.model.deploy(**charm_versions.s3.deploy_dict()),

    logger.info("Waiting for s3-integrator app to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.s3.application_name], timeout=1000
    )

    # Receive S3 params from fixture
    endpoint_url = s3_bucket_and_creds["endpoint"]
    access_key = s3_bucket_and_creds["access_key"]
    secret_key = s3_bucket_and_creds["secret_key"]
    bucket_name = s3_bucket_and_creds["bucket"]

    logger.info("Setting up s3 credentials in s3-integrator charm")
    s3_integrator_unit = ops_test.model.applications[charm_versions.s3.application_name].units[0]
    action = await s3_integrator_unit.run_action(
        action_name="sync-s3-credentials", **{"access-key": access_key, "secret-key": secret_key}
    )
    await action.wait()

    logger.info("Waiting for s3-integrator app to be idle and active...")
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[charm_versions.s3.application_name], status="active"
        )

    logger.info("Setting configuration for s3-integrator charm...")
    await ops_test.model.applications[charm_versions.s3.application_name].set_config(
        {
            "bucket": bucket_name,
            "path": "testpath",
            "endpoint": endpoint_url,
        }
    )

    logger.info("Integrating kyuubi charm with s3-integrator charm...")
    await ops_test.model.integrate(charm_versions.s3.application_name, APP_NAME)

    logger.info("Waiting for s3-integrator and kyuubi charms to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.s3.application_name], timeout=1000
    )

    # Assert that both kyuubi-k8s and s3-integrator charms are in active state
    assert ops_test.model.applications[APP_NAME].status == "active"
    assert ops_test.model.applications[charm_versions.s3.application_name].status == "active"


@pytest.mark.abort_on_fail
async def test_jdbc_endpoint_with_default_metastore(ops_test: OpsTest, test_pod):
    """Test the JDBC endpoint exposed by the charm."""
    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    kyuubi_unit = ops_test.model.applications[APP_NAME].units[0]
    action = await kyuubi_unit.run_action(
        action_name="get-jdbc-endpoint",
    )
    result = await action.wait()

    jdbc_endpoint = result.results.get("endpoint")
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    logger.info(
        "Testing JDBC endpoint by connecting with beeline" " and executing a few SQL queries..."
    )
    process = subprocess.run(
        [
            "./tests/integration/test_jdbc_endpoint.sh",
            test_pod,
            jdbc_endpoint,
            "db_default_metastore",
            "table_default_metastore",
        ],
        capture_output=True,
    )
    print("========== test_jdbc_endpoint.sh STDOUT =================")
    print(process.stdout.decode())
    print("========== test_jdbc_endpoint.sh STDERR =================")
    print(process.stderr.decode())
    logger.info(f"JDBC endpoint test returned with status {process.returncode}")
    assert process.returncode == 0


@pytest.mark.abort_on_fail
async def test_integration_with_postgresql_over_metastore_db(ops_test: OpsTest, charm_versions):
    """Test the charm by integrating it with postgresql-k8s charm."""
    # Deploy the charm and wait for waiting status
    logger.info("Deploying postgresql-k8s charm...")
    await ops_test.model.deploy(**charm_versions.postgres.deploy_dict()),

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.postgres.application_name], timeout=1000, status="active"
    )

    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm...")
    await ops_test.model.integrate(
        charm_versions.postgres.application_name, f"{APP_NAME}:metastore-db"
    )

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.s3.application_name], timeout=1000
    )

    # Assert that both kyuubi-k8s and postgresql-k8s charms are in active state
    assert ops_test.model.applications[APP_NAME].status == "active"
    assert ops_test.model.applications[charm_versions.postgres.application_name].status == "active"


@pytest.mark.abort_on_fail
async def test_jdbc_endpoint_with_postgres_metastore(ops_test: OpsTest, test_pod, charm_versions):
    """Test the JDBC endpoint exposed by the charm."""
    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    kyuubi_unit = ops_test.model.applications[APP_NAME].units[0]
    action = await kyuubi_unit.run_action(
        action_name="get-jdbc-endpoint",
    )
    result = await action.wait()

    jdbc_endpoint = result.results.get("endpoint")
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    logger.info(
        "Testing JDBC endpoint by connecting with beeline" " and executing a few SQL queries..."
    )
    process = subprocess.run(
        [
            "./tests/integration/test_jdbc_endpoint.sh",
            test_pod,
            jdbc_endpoint,
            "db_postgres_metastore",
            "table_postgres_metastore",
        ],
        capture_output=True,
    )
    print("========== test_jdbc_endpoint.sh STDOUT =================")
    print(process.stdout.decode())
    print("========== test_jdbc_endpoint.sh STDERR =================")
    print(process.stderr.decode())
    logger.info(f"JDBC endpoint test returned with status {process.returncode}")
    assert process.returncode == 0

    # Fetch password for default user from postgresql-k8s
    postgres_unit = ops_test.model.applications[charm_versions.postgres.application_name].units[0]
    action = await postgres_unit.run_action(
        action_name="get-password",
    )
    result = await action.wait()
    password = result.results.get("password")

    # Fetch host address of postgresql-k8s
    status = await ops_test.model.get_status()
    postgresql_host_address = status["applications"][charm_versions.postgres.application_name][
        "units"
    ][f"{charm_versions.postgres.application_name}/0"]["address"]

    # Connect to PostgreSQL metastore database
    connection = psycopg2.connect(
        host=postgresql_host_address,
        database=METASTORE_DATABASE_NAME,
        user="operator",
        password=password,
    )

    # Fetch number of new db and tables that have been added to metastore
    num_dbs = num_tables = 0
    with connection.cursor() as cursor:
        cursor.execute(""" SELECT * FROM "DBS" WHERE "NAME" = 'db_postgres_metastore' """)
        num_dbs = cursor.rowcount
        cursor.execute(""" SELECT * FROM "TBLS" WHERE "TBL_NAME" = 'table_postgres_metastore' """)
        num_tables = cursor.rowcount

    connection.close()

    # Assert that new database and tables have indeed been added to metastore
    assert num_dbs != 0
    assert num_tables != 0


@pytest.mark.abort_on_fail
async def test_jdbc_endpoint_after_removing_postgresql_metastore(
    ops_test: OpsTest, test_pod, charm_versions
):
    """Test the JDBC endpoint exposed by the charm."""
    logger.info("Removing relation between postgresql-k8s and kyuubi-k8s...")
    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:metastore-db", f"{charm_versions.postgres.application_name}:database"
    )

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.postgres.application_name], timeout=1000, status="active"
    )

    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    kyuubi_unit = ops_test.model.applications[APP_NAME].units[0]
    action = await kyuubi_unit.run_action(
        action_name="get-jdbc-endpoint",
    )
    result = await action.wait()

    jdbc_endpoint = result.results.get("endpoint")
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    logger.info(
        "Testing JDBC endpoint by connecting with beeline and executing a few SQL queries..."
    )
    process = subprocess.run(
        [
            "./tests/integration/test_jdbc_endpoint.sh",
            test_pod,
            jdbc_endpoint,
            "db_default_metastore_2",
            "table_default_metastore_2",
        ],
        capture_output=True,
    )
    print("========== test_jdbc_endpoint.sh STDOUT =================")
    print(process.stdout.decode())
    print("========== test_jdbc_endpoint.sh STDERR =================")
    print(process.stderr.decode())
    logger.info(f"JDBC endpoint test returned with status {process.returncode}")
    assert process.returncode == 0

    # Fetch password for default user from postgresql-k8s
    postgres_unit = ops_test.model.applications[charm_versions.postgres.application_name].units[0]
    action = await postgres_unit.run_action(
        action_name="get-password",
    )
    result = await action.wait()
    password = result.results.get("password")

    # Fetch host address of postgresql-k8s
    status = await ops_test.model.get_status()
    postgresql_host_address = status["applications"][charm_versions.postgres.application_name][
        "units"
    ][f"{charm_versions.postgres.application_name}/0"]["address"]

    # Connect to PostgreSQL metastore database
    connection = psycopg2.connect(
        host=postgresql_host_address,
        database=METASTORE_DATABASE_NAME,
        user="operator",
        password=password,
    )

    # Fetch number of new db and tables that have been added to metastore
    num_dbs = num_tables = 0
    with connection.cursor() as cursor:
        cursor.execute(""" SELECT * FROM "DBS" WHERE "NAME" = 'db_default_metastore_2' """)
        num_dbs = cursor.rowcount
        logger.info(cursor.fetchall())
        cursor.execute(""" SELECT * FROM "TBLS" WHERE "TBL_NAME" = 'table_default_metastore_2' """)
        num_tables = cursor.rowcount
        logger.info(cursor.fetchall())

    connection.close()

    # Assert that new database and tables are not created in PostgreSQL
    # (because the relation has already been removed.)
    assert num_dbs == 0
    assert num_tables == 0


@pytest.mark.abort_on_fail
async def test_enable_authentication(ops_test: OpsTest, charm_versions):
    """Test the the behavior of charm when authentication is enabled."""
    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.postgres.application_name], timeout=1000, status="active"
    )

    logger.info("Integrating kyuubi-k8s charm with postgresql-k8s charm over auth-db endpoint...")
    await ops_test.model.integrate(charm_versions.postgres.application_name, f"{APP_NAME}:auth-db")

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s charms to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.postgres.application_name], timeout=1000
    )

    # Assert that both kyuubi-k8s and postgresql-k8s charms are in active state
    assert ops_test.model.applications[APP_NAME].status == "active"
    assert ops_test.model.applications[charm_versions.postgres.application_name].status == "active"


@pytest.mark.abort_on_fail
async def test_jdbc_endpoint_no_credentials(ops_test: OpsTest, test_pod):
    """Test the JDBC connection when invalid credentials are provided."""
    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    kyuubi_unit = ops_test.model.applications[APP_NAME].units[0]
    action = await kyuubi_unit.run_action(
        action_name="get-jdbc-endpoint",
    )
    result = await action.wait()

    jdbc_endpoint = result.results.get("endpoint")
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    logger.info("Testing JDBC endpoint by connecting with beeline with no credentials ...")
    process = subprocess.run(
        [
            "./tests/integration/test_jdbc_endpoint.sh",
            test_pod,
            jdbc_endpoint,
            "db_111",
            "table_111",
        ],
        capture_output=True,
    )
    print("========== test_jdbc_endpoint.sh STDOUT =================")
    print(process.stdout.decode())
    print("========== test_jdbc_endpoint.sh STDERR =================")
    print(process.stderr.decode())
    logger.info(f"JDBC endpoint test returned with status {process.returncode}")
    assert process.returncode == 1
    assert "Error validating the login" in process.stderr.decode()


@pytest.mark.abort_on_fail
async def test_jdbc_endpoint_invalid_credentials(ops_test: OpsTest, test_pod):
    """Test the JDBC connection when invalid credentials are provided."""
    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    kyuubi_unit = ops_test.model.applications[APP_NAME].units[0]
    action = await kyuubi_unit.run_action(
        action_name="get-jdbc-endpoint",
    )
    result = await action.wait()

    jdbc_endpoint = result.results.get("endpoint")
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    username = "admin"
    password = str(uuid.uuid4())
    logger.info(
        f"Testing JDBC endpoint by connecting with beeline with username={username} and password={password} ..."
    )
    process = subprocess.run(
        [
            "./tests/integration/test_jdbc_endpoint.sh",
            test_pod,
            jdbc_endpoint,
            "db_222",
            "table_222",
            username,
            password,
        ],
        capture_output=True,
    )
    print("========== test_jdbc_endpoint.sh STDOUT =================")
    print(process.stdout.decode())
    print("========== test_jdbc_endpoint.sh STDERR =================")
    print(process.stderr.decode())
    logger.info(f"JDBC endpoint test returned with status {process.returncode}")
    assert process.returncode == 1
    assert "Error validating the login" in process.stderr.decode()


@pytest.mark.abort_on_fail
async def test_jdbc_endpoint_valid_credentials(ops_test: OpsTest, test_pod):
    """Test the JDBC connection when invalid credentials are provided."""
    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    kyuubi_unit = ops_test.model.applications[APP_NAME].units[0]
    action = await kyuubi_unit.run_action(
        action_name="get-jdbc-endpoint",
    )
    result = await action.wait()

    jdbc_endpoint = result.results.get("endpoint")
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    logger.info("Running action 'get-password' on kyuubi-k8s unit...")
    action = await kyuubi_unit.run_action(
        action_name="get-password",
    )
    result = await action.wait()

    password = result.results.get("password")
    logger.info(f"Fetched password: {password}")

    username = "admin"

    logger.info(
        f"Testing JDBC endpoint by connecting with beeline with username={username} and password={password} ..."
    )
    process = subprocess.run(
        [
            "./tests/integration/test_jdbc_endpoint.sh",
            test_pod,
            jdbc_endpoint,
            "db_333",
            "table_333",
            username,
            password,
        ],
        capture_output=True,
    )
    print("========== test_jdbc_endpoint.sh STDOUT =================")
    print(process.stdout.decode())
    print("========== test_jdbc_endpoint.sh STDERR =================")
    print(process.stderr.decode())
    logger.info(f"JDBC endpoint test returned with status {process.returncode}")
    assert process.returncode == 0


@pytest.mark.abort_on_fail
async def test_set_password_action(ops_test: OpsTest, test_pod):
    """Test set-password action."""
    logger.info("Running action 'get-password' on kyuubi-k8s unit...")
    kyuubi_unit = ops_test.model.applications[APP_NAME].units[0]
    action = await kyuubi_unit.run_action(
        action_name="get-password",
    )
    result = await action.wait()
    old_password = result.results.get("password")

    logger.info("Running action 'set-password' on kyuubi-k8s unit...")
    password_to_set = str(uuid.uuid4())
    action = await kyuubi_unit.run_action(action_name="set-password", password=password_to_set)
    result = await action.wait()
    assert result.results.get("password") == password_to_set

    logger.info("Running action 'get-password' on kyuubi-k8s unit...")
    action = await kyuubi_unit.run_action(
        action_name="get-password",
    )
    result = await action.wait()
    new_password = result.results.get("password")

    assert new_password != old_password
    assert new_password == password_to_set

    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    action = await kyuubi_unit.run_action(
        action_name="get-jdbc-endpoint",
    )
    result = await action.wait()

    jdbc_endpoint = result.results.get("endpoint")
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    username = "admin"

    logger.info(
        f"Testing JDBC endpoint by connecting with beeline with username={username} and password={new_password} ..."
    )
    process = subprocess.run(
        [
            "./tests/integration/test_jdbc_endpoint.sh",
            test_pod,
            jdbc_endpoint,
            "db_444",
            "table_444",
            username,
            new_password,
        ],
        capture_output=True,
    )
    print("========== test_jdbc_endpoint.sh STDOUT =================")
    print(process.stdout.decode())
    print("========== test_jdbc_endpoint.sh STDERR =================")
    print(process.stderr.decode())
    logger.info(f"JDBC endpoint test returned with status {process.returncode}")
    assert process.returncode == 0


@pytest.mark.abort_on_fail
async def test_kyuubi_client_relation_joined(ops_test: OpsTest, test_pod, charm_versions):
    logger.info("Building test charm (app-charm)...")
    app_charm = await ops_test.build_charm(TEST_CHARM_PATH)

    # Deploy the test charm and wait for waiting status
    logger.info("Deploying test charm...")
    await ops_test.model.deploy(
        app_charm,
        application_name=TEST_CHARM_NAME,
        num_units=1,
        series="jammy",
    )

    logger.info("Waiting for test charm to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[TEST_CHARM_NAME, APP_NAME], timeout=1000, status="active"
    )

    # Check number of users before integration
    # Fetch password for operator user from postgresql-k8s
    postgres_unit = ops_test.model.applications[charm_versions.postgres.application_name].units[0]
    action = await postgres_unit.run_action(
        action_name="get-password",
    )
    result = await action.wait()
    password = result.results.get("password")

    # Fetch host address of postgresql-k8s
    status = await ops_test.model.get_status()
    postgresql_host_address = status["applications"][charm_versions.postgres.application_name][
        "units"
    ][f"{charm_versions.postgres.application_name}/0"]["address"]

    # Connect to PostgreSQL metastore database
    connection = psycopg2.connect(
        host=postgresql_host_address,
        database=AUTHENTICATION_DATABASE_NAME,
        user="operator",
        password=password,
    )

    # Fetch number of users excluding the default admin user
    with connection.cursor() as cursor:
        cursor.execute(""" SELECT username, passwd FROM kyuubi_users WHERE username <> 'admin' """)
        num_users = cursor.rowcount

    assert num_users == 0

    logger.info("Integrating test charm with kyuubi-k8s charm...")
    await ops_test.model.integrate(TEST_CHARM_NAME, APP_NAME)

    logger.info("Waiting for test-charm and kyuubi charm to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, TEST_CHARM_NAME], timeout=1000, status="active"
    )

    # Fetch number of users excluding the default admin user
    with connection.cursor() as cursor:
        cursor.execute(""" SELECT username, passwd FROM kyuubi_users WHERE username <> 'admin' """)
        num_users = cursor.rowcount
        kyuubi_username, kyuubi_password = cursor.fetchone()

    connection.close()

    # Assert that a new user had indeed been created
    assert num_users != 0

    logger.info(f"Relation user's username: {kyuubi_username} and password: {kyuubi_password}")

    # Get JDBC endpoint
    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    kyuubi_unit = ops_test.model.applications[APP_NAME].units[0]
    action = await kyuubi_unit.run_action(
        action_name="get-jdbc-endpoint",
    )
    result = await action.wait()

    jdbc_endpoint = result.results.get("endpoint")
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    logger.info(
        "Testing JDBC endpoint by connecting with beeline and executing a few SQL queries..."
    )

    process = subprocess.run(
        [
            "./tests/integration/test_jdbc_endpoint.sh",
            test_pod,
            jdbc_endpoint,
            "db_666",
            "tbl_666",
            kyuubi_username,
            kyuubi_password,
        ],
        capture_output=True,
    )
    print("========== test_jdbc_endpoint.sh STDOUT =================")
    print(process.stdout.decode())
    print("========== test_jdbc_endpoint.sh STDERR =================")
    print(process.stderr.decode())
    logger.info(f"JDBC endpoint test returned with status {process.returncode}")
    assert process.returncode == 0


@pytest.mark.abort_on_fail
async def test_kyuubi_client_relation_removed(ops_test: OpsTest, test_pod, charm_versions):

    logger.info("Waiting for charms to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[TEST_CHARM_NAME, APP_NAME], timeout=1000, status="active"
    )

    # Fetch password for operator user from postgresql-k8s
    postgres_unit = ops_test.model.applications[charm_versions.postgres.application_name].units[0]
    action = await postgres_unit.run_action(
        action_name="get-password",
    )
    result = await action.wait()
    password = result.results.get("password")

    # Fetch host address of postgresql-k8s
    status = await ops_test.model.get_status()
    postgresql_host_address = status["applications"][charm_versions.postgres.application_name][
        "units"
    ][f"{charm_versions.postgres.application_name}/0"]["address"]

    # Connect to PostgreSQL metastore database
    connection = psycopg2.connect(
        host=postgresql_host_address,
        database=AUTHENTICATION_DATABASE_NAME,
        user="operator",
        password=password,
    )

    # Fetch number of users excluding the default admin user
    with connection.cursor() as cursor:
        cursor.execute(""" SELECT username, passwd FROM kyuubi_users WHERE username <> 'admin' """)
        num_users_before = cursor.rowcount
        kyuubi_username, kyuubi_password = cursor.fetchone()

    assert num_users_before != 0

    logger.info("Removing relation between test charm and kyuubi-k8s...")
    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:{KYUUBI_CLIENT_RELATION_NAME}",
        f"{TEST_CHARM_NAME}:{KYUUBI_CLIENT_RELATION_NAME}",
    )

    logger.info("Waiting for test-charm and kyuubi charm to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, TEST_CHARM_NAME], timeout=1000, status="active"
    )

    # Fetch number of users excluding the default admin user
    with connection.cursor() as cursor:
        cursor.execute(""" SELECT username, passwd FROM kyuubi_users WHERE username <> 'admin' """)
        num_users_after = cursor.rowcount

    connection.close()

    # Assert that a new user had indeed been created
    assert num_users_after == 0

    # Get JDBC endpoint
    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    kyuubi_unit = ops_test.model.applications[APP_NAME].units[0]
    action = await kyuubi_unit.run_action(
        action_name="get-jdbc-endpoint",
    )
    result = await action.wait()

    jdbc_endpoint = result.results.get("endpoint")
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    logger.info(
        "Testing JDBC endpoint by connecting with beeline and executing a few SQL queries..."
    )
    process = subprocess.run(
        [
            "./tests/integration/test_jdbc_endpoint.sh",
            test_pod,
            jdbc_endpoint,
            "db_777",
            "tbl_777",
            kyuubi_username,
            kyuubi_password,
        ],
        capture_output=True,
    )
    print("========== test_jdbc_endpoint.sh STDOUT =================")
    print(process.stdout.decode())
    print("========== test_jdbc_endpoint.sh STDERR =================")
    print(process.stderr.decode())
    logger.info(f"JDBC endpoint test returned with status {process.returncode}")
    assert process.returncode == 1
    assert "Error validating the login" in process.stderr.decode()


@pytest.mark.abort_on_fail
async def test_remove_authentication(ops_test: OpsTest, test_pod, charm_versions):
    """Test the JDBC connection when authentication is disabled."""
    logger.info("Removing relation between postgresql-k8s and kyuubi-k8s over auth-db endpoint...")
    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:auth-db", f"{charm_versions.postgres.application_name}:database"
    )

    logger.info("Waiting for postgresql-k8s and kyuubi-k8s apps to be idle and active...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.postgres.application_name], timeout=1000, status="active"
    )
    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    kyuubi_unit = ops_test.model.applications[APP_NAME].units[0]
    action = await kyuubi_unit.run_action(
        action_name="get-jdbc-endpoint",
    )
    result = await action.wait()

    jdbc_endpoint = result.results.get("endpoint")
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    logger.info("Testing JDBC endpoint by connecting with beeline with no credentials ...")
    process = subprocess.run(
        [
            "./tests/integration/test_jdbc_endpoint.sh",
            test_pod,
            jdbc_endpoint,
            "db_555",
            "table_555",
        ],
        capture_output=True,
    )
    print("========== test_jdbc_endpoint.sh STDOUT =================")
    print(process.stdout.decode())
    print("========== test_jdbc_endpoint.sh STDERR =================")
    print(process.stderr.decode())
    logger.info(f"JDBC endpoint test returned with status {process.returncode}")
    assert process.returncode == 0


@pytest.mark.abort_on_fail
async def test_read_spark_properties_from_secrets(ops_test: OpsTest, test_pod, service_account):
    """Test that the spark properties provided via K8s secrets (spark8t library) are picked by Kyuubi."""
    namespace, _ = service_account
    sa_name = "custom_sa"

    # Adding a custom property via Spark8t to the service account
    assert (
        subprocess.run(
            [
                "python",
                "-m" "spark8t.cli.service_account_registry",
                "create",
                "--username",
                sa_name,
                "--namespace",
                namespace,
                "--conf",
                "spark.executor.instances=3",
            ],
        ).returncode
        == 0
    )

    logger.info("Changing configuration for kyuubi-k8s charm...")
    await ops_test.model.applications[APP_NAME].set_config(
        {"namespace": namespace, "service-account": sa_name}
    )

    logger.info("Waiting for kyuubi-k8s app to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="blocked",
        timeout=1000,
    )

    logger.info("Running action 'get-jdbc-endpoint' on kyuubi-k8s unit...")
    kyuubi_unit = ops_test.model.applications[APP_NAME].units[0]
    action = await kyuubi_unit.run_action(
        action_name="get-jdbc-endpoint",
    )
    result = await action.wait()

    jdbc_endpoint = result.results.get("endpoint")
    logger.info(f"JDBC endpoint: {jdbc_endpoint}")

    logger.info("Testing JDBC endpoint by connecting with beeline with no credentials ...")
    process = subprocess.run(
        [
            "./tests/integration/test_jdbc_endpoint.sh",
            test_pod,
            jdbc_endpoint,
            "db_555",
            "table_555",
        ],
        capture_output=True,
    )
    print("========== test_jdbc_endpoint.sh STDOUT =================")
    print(process.stdout.decode())
    print("========== test_jdbc_endpoint.sh STDERR =================")
    print(process.stderr.decode())
    logger.info(f"JDBC endpoint test returned with status {process.returncode}")
    assert process.returncode == 0

    logger.info("Sleeping for a while...")
    import time

    time.sleep(100)

    # Check exactly 3 executor pods were created.
    list_pods_process = subprocess.run(
        ["kubectl", "get", "pods", "-n", namespace, "--sort-by", ".metadata.creationTimestamp"],
        capture_output=True,
    )

    assert list_pods_process.returncode == 0

    pods_list = list_pods_process.stdout.decode().splitlines()

    driver_pod_name = ""
    executor_pod_names = []
    for pod in pods_list[-4:]:
        name = pod.split()[0]
        if "driver" in name:
            driver_pod_name = name
        else:
            executor_pod_names.append(name)

    expected_executor_pod_names = [
        driver_pod_name.replace("driver", "exec-1"),
        driver_pod_name.replace("driver", "exec-2"),
        driver_pod_name.replace("driver", "exec-3"),
    ]

    assert len(executor_pod_names) == len(expected_executor_pod_names)
    assert set(executor_pod_names) == set(expected_executor_pod_names)


@pytest.mark.abort_on_fail
async def test_invalid_config(
    ops_test: OpsTest,
):
    """Test the behavior of charm when the  config provided to it are invalid."""
    logger.info("Setting invalid configuration for kyuubi-k8s charm...")
    await ops_test.model.applications[APP_NAME].set_config(
        {"namespace": "invalid", "service-account": "invalid"}
    )

    logger.info("Waiting for kyuubi-k8s app to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        timeout=1000,
    )

    # Assert that the charm is in blocked state, due to invalid config options
    assert ops_test.model.applications[APP_NAME].status == "blocked"
