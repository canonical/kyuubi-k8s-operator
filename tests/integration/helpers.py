import logging
from pathlib import Path
import subprocess
import uuid
import yaml

from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
TEST_CHARM_PATH = "./tests/integration/app-charm"
TEST_CHARM_NAME = "application"


def get_random_name():
    return str(uuid.uuid4()).replace("-", "_")


async def run_sql_against_jdbc_endpoint(ops_test: OpsTest, test_pod):
    """Test the JDBC endpoint exposed by the charm with some SQL queries."""

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
            ops_test.model_name,
            jdbc_endpoint,
            get_random_name(),
            get_random_name(),
        ],
        capture_output=True,
    )
    print("========== test_jdbc_endpoint.sh STDOUT =================")
    print(process.stdout.decode())
    print("========== test_jdbc_endpoint.sh STDERR =================")
    print(process.stderr.decode())
    logger.info(f"JDBC endpoint test returned with status {process.returncode}")
    return process.returncode == 0