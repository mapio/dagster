from dagster import op, job
from typing import Dict, Any, Tuple


def test_dict_input():
    @op
    def the_op(x: Dict[str, str]):
        assert x == {"foo": "bar"}

    @job
    def the_job():
        the_op()

    assert the_job.execute_in_process(
        run_config={
            "ops": {
                "the_op": {
                    "inputs": {
                        "x": {
                            "value": {
                                "foo": "bar",
                            }
                        },
                    }
                }
            }
        }
    ).success

    @job
    def the_job_top_lvl_input(x):
        the_op(x)

    assert the_job_top_lvl_input.execute_in_process(
        run_config={"inputs": {"x": {"value": {"foo": "bar"}}}}
    ).success


def test_any_dict_input():
    @op
    def the_op(x: Dict[str, Any]):
        assert x == {"foo": "bar"}

    @job
    def the_job():
        the_op()

    assert the_job.execute_in_process(
        run_config={
            "ops": {
                "the_op": {
                    "inputs": {
                        "x": {
                            "value": {
                                "foo": "bar",
                            }
                        },
                    }
                }
            }
        }
    ).success

    @job
    def the_job_top_lvl_input(x):
        the_op(x)

    assert the_job_top_lvl_input.execute_in_process(
        run_config={"inputs": {"x": {"value": {"foo": "bar"}}}}
    ).success


def test_nested_dict_input():
    @op
    def the_op(x: Dict[str, Dict[str, Any]]):
        assert x == {"value": {"foo": "bar"}}

    @job
    def the_job(x):
        the_op(x)

    assert the_job.execute_in_process(
        run_config={"inputs": {"x": {"value": {"value": {"foo": "bar"}}}}}
    ).success
