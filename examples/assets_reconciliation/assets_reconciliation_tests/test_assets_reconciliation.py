from dagster import execute_pipeline

from ..repo import my_pipeline


def test_assets_reconciliation():
    assert execute_pipeline(my_pipeline).success
