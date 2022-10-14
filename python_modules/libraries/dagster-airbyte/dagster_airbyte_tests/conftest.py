from datetime import datetime, timedelta
import time
import pytest
from contextlib import contextmanager
import subprocess
from dagster._utils import file_relative_path
import os
import requests
from dagster._core.test_utils import environ

pytest_plugins = ["dagster_test.fixtures"]


@pytest.fixture(name="docker_compose_file")
def docker_compose_file_fixture():
    return file_relative_path(__file__, "docker-compose.yml")


@pytest.fixture(name="docker_compose_env_file")
def docker_compose_env_file_fixture():
    return file_relative_path(__file__, "docker-compose.env")


RETRY_DELAY_SEC = 5
STARTUP_TIME_SEC = 60
AIRBYTE_VOLUMES = [
    "airbyte_integration_tests_data",
    "airbyte_integration_tests_db",
    "airbyte_integration_tests_workspace",
]


def _cleanup_docker(docker_compose_file, docker_compose_env_file):
    subprocess.check_output(
        ["docker-compose", "--env-file", docker_compose_env_file, "-f", docker_compose_file, "stop"]
    )
    subprocess.check_output(
        [
            "docker-compose",
            "--env-file",
            docker_compose_env_file,
            "-f",
            docker_compose_file,
            "rm",
            "-f",
        ]
    )
    subprocess.check_output(["docker", "volume", "rm"] + AIRBYTE_VOLUMES)


@pytest.fixture(name="docker_compose_airbyte_instance")
def docker_compose_airbyte_instance_fixture(
    docker_compose_cm, docker_compose_file, docker_compose_env_file
):
    """
    Spins up an Airbyte instance using docker-compose, and tears it down after the test.
    """

    with docker_compose_cm(docker_compose_file, env_file=docker_compose_env_file) as hostnames:

        # Poll Airbyte API until it's ready
        # Healthcheck endpoint is ready before API is ready, so we poll the API
        start_time = datetime.now()
        while True:
            now = datetime.now()
            if (now - start_time).seconds > STARTUP_TIME_SEC:
                raise Exception("Airbyte instance failed to start in time")

            try:
                poll_result = requests.post(
                    "http://localhost:8000/api/v1/workspaces/list",
                    headers={"Content-Type": "application/json"},
                )
                if poll_result.status_code == 200:
                    break
            except requests.exceptions.ConnectionError:
                pass

            time.sleep(RETRY_DELAY_SEC)
            print(
                "Waiting for Airbyte instance to start"
                + "." * (3 + (now - start_time).seconds // RETRY_DELAY_SEC)
            )

        with environ({"AIRBYTE_HOSTNAME": hostnames["airbyte-webapp"]}):
            yield hostnames["airbyte-webapp"]


@pytest.fixture(name="airbyte_source_files")
def airbyte_source_files_fixture():
    FILES = ["sample_file.json"]

    for file in FILES:
        with open(file_relative_path(__file__, file), "r") as f:
            contents = f.read()
        with open(os.path.join("/tmp/airbyte_local", file), "w") as f:
            f.write(contents)
