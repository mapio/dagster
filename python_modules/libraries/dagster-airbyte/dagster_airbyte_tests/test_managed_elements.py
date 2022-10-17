# pylint: disable=unused-argument


import os

import pytest
from dagster_airbyte import airbyte_resource, load_assets_from_connections
from dagster_managed_elements import ManagedElementDiff
from dagster_managed_elements.cli import apply, check
from dagster_managed_elements.utils import diff_dicts

from dagster import AssetKey, materialize
from dagster._utils import file_relative_path

from .example_stacks import example_airbyte_stack


def test_basic_integration(docker_compose_airbyte_instance, airbyte_source_files):

    ab_instance = airbyte_resource.configured(
        {
            "host": os.getenv("AIRBYTE_HOSTNAME"),
            "port": os.getenv("AIRBYTE_PORT"),
        }
    )
    ab_cacheable_assets = load_assets_from_connections(
        ab_instance, [example_airbyte_stack.local_json_conn]
    )

    with pytest.raises(ValueError):
        # Cannot load assets from connections because they haven't been created yet
        ab_assets = ab_cacheable_assets.build_definitions(
            ab_cacheable_assets.compute_cacheable_data()
        )

    # First, check that we get the expected diff

    check_result = check(file_relative_path(__file__, "./example_stacks/example_airbyte_stack.py"))

    config_dict = {
        "local-json-input": {
            "url": "/local/sample_file.json",
            "format": "json",
            "provider": {"storage": "local"},
            "dataset_name": "my_data_stream",
        },
        "local-json-output": {
            "destination_path": "/local/destination_file.json",
        },
        "local-json-conn": {
            "source": "local-json-input",
            "destination": "local-json-output",
            "normalize data": False,
            "streams": {
                "my_data_stream": "FULL_REFRESH_APPEND",
            },
        },
    }
    expected_result = diff_dicts(
        config_dict,
        None,
    )

    assert expected_result == check_result

    # Then, apply the diff and check that we get the expected diff again

    apply_result = apply(file_relative_path(__file__, "./example_stacks/example_airbyte_stack.py"))

    assert expected_result == apply_result

    # Now, check that we get no diff after applying the stack

    check_result = check(file_relative_path(__file__, "./example_stacks/example_airbyte_stack.py"))

    assert check_result == ManagedElementDiff()

    # Test that we can load assets from connections
    ab_assets = ab_cacheable_assets.build_definitions(ab_cacheable_assets.compute_cacheable_data())

    tables = {"my_data_stream"}
    assert ab_assets[0].keys == {AssetKey(t) for t in tables}

    res = materialize(ab_assets)

    materializations = [
        event.event_specific_data.materialization
        for event in res.all_events
        if event.event_type_value == "ASSET_MATERIALIZATION"
    ]
    assert len(materializations) == len(tables)
    assert {m.asset_key for m in materializations} == {AssetKey(t) for t in tables}

    # Ensure that the empty stack w/o delete has no diff (it will not try to delete resources it
    # doesn't know about)
    check_result = check(
        file_relative_path(__file__, "./example_stacks/empty_airbyte_stack_no_delete.py")
    )

    # Inverted result (e.g. all deletions)
    expected_result = ManagedElementDiff()

    # Now, we try to remove everything
    check_result = check(file_relative_path(__file__, "./example_stacks/empty_airbyte_stack.py"))

    # Inverted result (e.g. all deletions)
    expected_result = diff_dicts(
        None,
        config_dict,
    )

    assert expected_result == check_result

    # Then, apply the diff to remove everything and check that we get the expected diff again

    apply_result = apply(file_relative_path(__file__, "./example_stacks/empty_airbyte_stack.py"))

    assert expected_result == apply_result

    # Now, check that we get no diff after applying the stack

    check_result = check(file_relative_path(__file__, "./example_stacks/empty_airbyte_stack.py"))

    assert check_result == ManagedElementDiff()

    with pytest.raises(ValueError):
        # Cannot load assets from connections because they have been deleted
        ab_assets = ab_cacheable_assets.build_definitions(
            ab_cacheable_assets.compute_cacheable_data()
        )
