import os
from typing import List

import dagster._check as check
from dagster import Field, StringSource
from dagster._core.storage.captured_log_manager import CapturedLogMetadata
from dagster._core.storage.noop_compute_log_manager import NoOpComputeLogManager
from dagster._serdes import ConfigurableClass, ConfigurableClassData


class CloudwatchComputeLogManager(NoOpComputeLogManager, ConfigurableClass):
    """
    ComputeLogManager that directs dagit to an external Cloudwatch URL to view compute logs rather
    than capturing them directly.  To be used in tandem with the EcsRunLauncher.
    """

    def __init__(self, inst_data=None, project_name="dagster"):
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)
        self._project_name = check.str_param(project_name, "project_name")
        super().__init__(inst_data)

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {
            "project_name": Field(StringSource, is_required=False, default_value="dagster"),
        }

    @staticmethod
    def from_config_value(inst_data, config_value):
        return CloudwatchComputeLogManager(inst_data=inst_data, **config_value)

    def get_contextual_log_metadata(
        self, log_key: List[str]
    ) -> CapturedLogMetadata:  # pylint: disable=unused-argument
        metadata_uri = os.getenv("ECS_CONTAINER_METADATA_URI")
        region = os.getenv("AWS_REGION")
        if not metadata_uri or not region:
            return CapturedLogMetadata()

        arn_id = metadata_uri.split("/")[-1].split("-")[0]
        base_url = f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}"
        log_group = f"$252Fdocker-compose$252F{self._project_name}"
        log_name = f"{self._project_name}$252Frun$252F{arn_id}"
        return CapturedLogMetadata(
            external_url=f"{base_url}#logsV2:log-groups/log-group/{log_group}/log-events/{log_name}"
        )
