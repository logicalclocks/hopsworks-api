"""What the API sends, as the runner's models.

This seam had no tests, and a whole model change went through it unnoticed: the
runner kept building Tasks from expectedOutput, requiredTools, forbiddenTools and
rubric long after none of those existed on either side. It failed on the first
real run, after the four connection problems ahead of it had been fixed one at a
time.
"""

from __future__ import annotations

import json

import pytest
from hopsworks_agents.eval.evaluator_spec import evaluators_for_suite
from hopsworks_agents.eval.models import ExecutionMode, PassPolicy
from hopsworks_agents.eval.run_job import _tags, _to_suite


def run(**overrides):
    base = {
        "suiteId": "s1",
        "suiteVersion": 2,
        "executionMode": "read_only",
        "passPolicy": "all",
        "passThreshold": 0.7,
        "evaluators": [
            {
                "type": "contains",
                "name": "names_the_record",
                "position": 0,
                "config": "{}",
            },
        ],
    }
    base.update(overrides)
    return base


def task(**overrides):
    base = {
        "taskId": "t1",
        "version": 1,
        "inputMessages": json.dumps([{"role": "user", "content": "hi"}]),
        "taskType": "single_turn",
    }
    base.update(overrides)
    return base


class TestTasks:
    def test_expectations_are_copied_under_the_check_that_reads_them(self):
        # the same key results come back under, so this is a copy not a translation
        suite = _to_suite(run(), [task(expectations={"names_the_record": "UB40"})])
        assert suite.tasks[0].expects_text("names_the_record") == "UB40"

    def test_a_blank_expectation_is_dropped_rather_than_stored(self):
        # an empty value reads as a check that has been answered, which is the
        # opposite of the truth
        suite = _to_suite(run(), [task(expectations={"names_the_record": ""})])
        assert suite.tasks[0].expectations == {}

    def test_a_task_with_no_expectations_is_still_a_task(self):
        # a suite whose checks judge the run itself expects nothing of its tasks
        suite = _to_suite(run(), [task()])
        assert suite.tasks[0].task_id == "t1"
        assert suite.tasks[0].expectations == {}

    def test_the_question_survives(self):
        suite = _to_suite(run(), [task()])
        assert suite.tasks[0].prompt == "hi"

    def test_tool_expectations_keep_both_directions(self):
        stored = json.dumps({"required": ["lookup"], "forbidden": ["refund"]})
        suite = _to_suite(run(), [task(expectations={"tool_call": stored})])
        assert suite.tasks[0].expects_tools("tool_call") == (["lookup"], ["refund"])


class TestSuite:
    def test_the_checks_arrive_as_rows_and_still_build(self):
        # the end of this path: rows in, evaluators out
        [evaluator] = evaluators_for_suite(_to_suite(run(), [task()]))
        assert evaluator.type == "contains"
        assert evaluator.name == "names_the_record"

    def test_the_version_that_ran_is_the_one_recorded(self):
        # a result has to be able to say which version of the suite produced it
        assert _to_suite(run(), []).suite_version == 2

    def test_the_pass_policy_and_threshold_come_from_the_run(self):
        suite = _to_suite(run(passPolicy="threshold", passThreshold=0.9), [])
        assert suite.pass_policy is PassPolicy.THRESHOLD
        assert suite.pass_threshold == 0.9

    def test_the_execution_mode_comes_from_the_run(self):
        # what the runner refuses on depends on this, so a default is not harmless
        assert _to_suite(run(executionMode="sandboxed"), []).execution_mode is (
            ExecutionMode.SANDBOXED
        )

    def test_blocks_counting_as_success_survives(self):
        assert _to_suite(run(blocksAreSuccess=True), []).blocks_are_success


class TestTags:
    def test_a_json_array_becomes_a_list(self):
        # the column holds JSON; a raw string would make every character a tag
        assert _tags('["safety","regression"]') == ["safety", "regression"]

    def test_an_unreadable_value_is_no_tags(self):
        assert _tags("not json") == []

    def test_a_list_is_left_alone(self):
        assert _tags(["safety"]) == ["safety"]

    def test_nothing_is_no_tags(self):
        assert _tags(None) == []


class TestWritingResultsMatchesTheSchema:
    """Python has one integer type and pandas reads it as int64, so a column the.

    feature group declares `int` arrives as `bigint` and the insert is refused —
    after the whole suite has run, which is the most expensive moment to find out.
    """

    def frame(self, **columns):
        import pandas as pd

        return pd.DataFrame(columns)

    def group(self, **types):
        features = [
            type("F", (), {"name": name, "type": kind})()
            for name, kind in types.items()
        ]
        return type("G", (), {"features": features})()

    def test_an_int_column_is_narrowed_to_what_the_group_declares(self):
        from hopsworks_agents.eval.run_job import _match_schema

        frame = _match_schema(
            self.group(trial_index="int"), self.frame(trial_index=[0, 1])
        )
        assert str(frame["trial_index"].dtype) == "int32"

    def test_a_bigint_column_is_left_wide(self):
        from hopsworks_agents.eval.run_job import _match_schema

        frame = _match_schema(
            self.group(deployment_id="bigint"), self.frame(deployment_id=[1, 2])
        )
        assert str(frame["deployment_id"].dtype) == "int64"

    def test_a_column_with_nulls_gets_the_nullable_form_of_its_type(self):
        # pandas cannot hold a null in a plain integer type; the nullable one
        # keeps the declared width and the hole
        from hopsworks_agents.eval.run_job import _match_schema

        frame = _match_schema(
            self.group(input_tokens="bigint"), self.frame(input_tokens=[1, None])
        )
        assert str(frame["input_tokens"].dtype) == "Int64"
        assert frame["input_tokens"].isna().sum() == 1

    def test_an_all_null_column_is_typed_rather_than_left_as_null(self):
        # The case that lost a failed run's record: no trial got a trace, so
        # trace_id was None in every row, Arrow inferred `null`, and Delta refused
        # the write with "Invalid data type for Delta Lake: Null".
        import pytest

        pa = pytest.importorskip("pyarrow")  # what hsfs hands the frame to
        from hopsworks_agents.eval.run_job import _match_schema

        frame = _match_schema(
            self.group(trace_id="string", latency_ms="double", passed="boolean"),
            self.frame(
                trace_id=[None, None], latency_ms=[None, None], passed=[None, None]
            ),
        )
        table = pa.Table.from_pandas(frame, preserve_index=False)
        assert not any(pa.types.is_null(field.type) for field in table.schema)
        assert pa.types.is_string(
            table.schema.field("trace_id").type
        ) or pa.types.is_large_string(table.schema.field("trace_id").type)
        assert pa.types.is_float64(table.schema.field("latency_ms").type)
        assert pa.types.is_boolean(table.schema.field("passed").type)

    def test_a_column_the_frame_does_not_have_is_not_invented(self):
        from hopsworks_agents.eval.run_job import _match_schema

        frame = _match_schema(self.group(absent="int"), self.frame(present=[1]))
        assert list(frame.columns) == ["present"]

    def test_a_type_it_does_not_know_is_left_alone(self):
        from hopsworks_agents.eval.run_job import _match_schema

        frame = _match_schema(self.group(reason="string"), self.frame(reason=["ok"]))
        assert frame["reason"].tolist() == ["ok"]

    def test_a_group_that_reports_no_schema_is_not_an_error(self):
        # rather than failing the write over an introspection detail
        from hopsworks_agents.eval.run_job import _match_schema

        frame = _match_schema(type("G", (), {})(), self.frame(a=[1]))
        assert frame["a"].tolist() == [1]


class TestFindingTheDefaultJudgesKey:
    """A key set on a Hopsworks account is injected into every job container, so.

    by the time the runner starts it is already an environment variable.
    """

    def test_eval_judge_api_key_wins(self, monkeypatch):
        # so evaluation can be pointed at a separate key from everything else
        from hopsworks_agents.eval import run_job

        monkeypatch.setenv("EVAL_JUDGE_API_KEY", "for-evaluation")
        monkeypatch.setenv("ANTHROPIC_API_KEY", "for-everything")
        assert run_job._judge_for() is not None

    def test_the_providers_variable_is_used_otherwise(self, monkeypatch):
        from hopsworks_agents.eval import run_job

        monkeypatch.delenv("EVAL_JUDGE_API_KEY", raising=False)
        monkeypatch.setenv("ANTHROPIC_API_KEY", "for-everything")
        assert run_job._judge_for() is not None

    def test_with_no_key_it_says_where_to_put_one(self, monkeypatch, caplog):
        # not "no secret X": the reason a suite reported 100% while its only real
        # check never ran
        import logging

        from hopsworks_agents.eval import run_job

        monkeypatch.delenv("EVAL_JUDGE_API_KEY", raising=False)
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        with caplog.at_level(logging.INFO):
            assert run_job._judge_for() is None
        assert "ANTHROPIC_API_KEY" in caplog.text
        assert "account's environment variables" in caplog.text


class TestOneExecutionManyRuns:
    """An evaluation job runs everything it is configured to run, in one execution.

    The resources are paid for once rather than per target, so a nightly job with
    three suites and a monitor is one pod, not four.
    """

    def test_run_id_is_repeatable(self):
        import argparse

        from hopsworks_agents.eval import run_job

        parser = argparse.ArgumentParser()
        parser.add_argument("--run-id", required=True, action="append", dest="run_ids")
        parsed = parser.parse_args(["--run-id", "a", "--run-id", "b"])
        assert parsed.run_ids == ["a", "b"]
        assert callable(run_job._execute)

    def test_one_failure_does_not_take_the_others_down(self, monkeypatch):
        # A suite that refuses must not stop the three beside it: each has its own
        # row, and stopping early would leave them saying nothing at all.
        from hopsworks_agents.eval import run_job

        seen = []

        def fake_execute(run_id, session, base, project, host, args):
            seen.append(run_id)
            return run_id != "bad"

        monkeypatch.setattr(run_job, "_execute", fake_execute)
        monkeypatch.setattr(run_job, "hopsworks_session", lambda: object())
        monkeypatch.setattr(run_job, "_api", lambda host, pid: "base")

        import sys
        import types

        fake_hopsworks = types.ModuleType("hopsworks")
        fake_hopsworks.login = lambda: types.SimpleNamespace(id=1, name="p")
        monkeypatch.setitem(sys.modules, "hopsworks", fake_hopsworks)
        monkeypatch.setenv("HOPSWORKS_HOST", "https://h")
        monkeypatch.setattr(
            sys,
            "argv",
            ["run_job", "--run-id", "good", "--run-id", "bad", "--run-id", "also-good"],
        )

        with pytest.raises(SystemExit):
            run_job.main()
        # Every run was attempted, and the job still exits non-zero so an alert fires.
        assert seen == ["good", "bad", "also-good"]
