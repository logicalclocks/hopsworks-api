"""User-defined checks: a source the project wrote, every check it declares run.

The spec never carries code. A row names a file, a repository or a module;
the job resolves that, imports it, and runs every function declared with
`@check`, each reported under its own name -- the way a test file is pointed
at and every test in it runs. Everything a check can get wrong -- raising,
hanging, returning nonsense, not existing -- is the harness failing, never a
pass.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import textwrap
import time
from pathlib import Path

import pytest
from hopsworks_agents.eval import checks as checks_module
from hopsworks_agents.eval.checks import (
    CheckContext,
    CheckResult,
    PythonSourceEvaluator,
    ToolCall,
    _CheckSources,
    _declared_in,
    _import_path,
    _parse_source,
    _validate_entry,
    check,
    run_check,
    ungradable,
)
from hopsworks_agents.eval.evaluator_spec import (
    SpecError,
    _evaluators_from_spec,
    _validate_spec,
)
from hopsworks_agents.eval.evaluators import SKIPPED, _run_evaluators
from hopsworks_agents.eval.models import Task, Trial


def _task(**overrides) -> Task:
    base = {
        "task_id": "t1",
        "input_messages": json.dumps([{"role": "user", "content": "how much?"}]),
        "expectations": {},
    }
    base.update(overrides)
    return Task(**base)


def _trial(answer: str = "It costs 19.99") -> Trial:
    return Trial(
        trial_id="x",
        run_id="r",
        task_id="t1",
        task_version=1,
        trial_index=0,
        deployment_id=1,
        final_output=answer,
    )


def _trace(*calls: dict) -> dict:
    tool_calls = [
        {
            "name": c.get("name", "tool"),
            "arguments": c.get("arguments", ""),
            "result": c.get("result", ""),
            "status": c.get("status", "OK"),
            "duration_ms": c.get("duration_ms"),
            "call_id": "",
            "span_id": "s",
        }
        for c in calls
    ]
    return {
        "trace_id": "abc",
        "root_span_id": "root",
        "tool_calls": tool_calls,
        "tool_names": [c["name"] for c in tool_calls],
        "tool_error_count": sum(1 for c in tool_calls if c["status"].endswith("ERROR")),
    }


def _rows(evaluators, task=None, trial=None, trace=None) -> dict:
    """The results of one trial, by the name they report under."""
    results = _run_evaluators(evaluators, task or _task(), trial or _trial(), trace)
    by_name = {}
    for result in results:
        assert result.evaluator_name not in by_name, (
            f"two rows named {result.evaluator_name}"
        )
        by_name[result.evaluator_name] = result
    return by_name


CHECK_FILE = """
import json
from hopsworks_agents.eval.checks import check, CheckResult, ungradable

@check(needs_trace=True, description="Quoted price equals the catalogue price")
def price_matches_catalogue(ctx, tolerance: float = 0.01):
    quoted = float(ctx.answer.split()[-1])
    lookups = ctx.calls("lookup_product")
    if not lookups:
        return ungradable("the agent never looked the product up")
    listed = float(json.loads(lookups[-1].result)["price"])
    ok = abs(quoted - listed) <= tolerance
    return CheckResult(passed=ok, score=1.0 if ok else 0.0,
                       reason="" if ok else f"quoted {quoted}, catalogue says {listed}",
                       assertions={"quoted": quoted, "listed": listed})

@check("answers_in_english", needs_trace=False)
def _english(ctx):
    return "the" in ctx.answer.lower()

def helper_not_a_check(ctx):
    return True
"""

DECLARED = {"price_matches_catalogue", "answers_in_english"}


@pytest.fixture
def check_file(tmp_path: Path) -> Path:
    path = tmp_path / "pricing.py"
    path.write_text(textwrap.dedent(CHECK_FILE))
    return path


def _write(tmp_path: Path, name: str, body: str) -> Path:
    path = tmp_path / name
    path.write_text(
        "from hopsworks_agents.eval.checks import check, CheckResult\n"
        + textwrap.dedent(body)
    )
    return path


# ── declaring ───────────────────────────────────────────────────────────────


class TestDecorator:
    def test_bare_named_and_configured_forms(self, check_file):
        module, _held, _sub, _read = _import_path(str(check_file))
        declared = _declared_in(module)
        assert set(declared) == DECLARED
        price = declared["price_matches_catalogue"]
        assert price.needs_trace is True
        assert price.defaults == {"tolerance": 0.01}
        assert price.description == "Quoted price equals the catalogue price"
        assert declared["answers_in_english"].needs_trace is False

    def test_bare_decorator_takes_the_function_name(self):
        @check
        def says_hello(ctx):
            return "hello" in ctx.answer

        declared = getattr(says_hello, checks_module._MARK)
        assert declared.name == "says_hello"
        assert declared.defaults == {}

    def test_docstring_is_the_default_description(self):
        @check
        def documented(ctx):
            """Says what it checks."""
            return True

        assert getattr(documented, checks_module._MARK).description == (
            "Says what it checks."
        )

    def test_timeout_is_bounded(self):
        @check(timeout_s=10_000)
        def patient(ctx):
            return True

        assert getattr(patient, checks_module._MARK).timeout_s == 600


# ── the spec ────────────────────────────────────────────────────────────────


class TestSpec:
    def test_python_is_a_known_type_and_validates_without_io(self):
        _validate_spec(
            json.dumps(
                [
                    {"type": "python", "name": "a", "file": "Resources/x.py"},
                    {"type": "python", "name": "b", "module": "pkg.checks"},
                    {
                        "type": "python",
                        "name": "c",
                        "git": {"url": "https://github.com/o/r.git", "ref": "main"},
                        "path": "evaluation/checks.py",
                        "only": ["price"],
                        "params": {"price": {"tolerance": 1}},
                    },
                    {
                        "type": "python",
                        "name": "d",
                        "git": "deployment",
                        "path": "c.py",
                    },
                ]
            )
        )

    @pytest.mark.parametrize(
        ("entry", "reason"),
        [
            ({"type": "python", "name": "a"}, "exactly one of file, module and git"),
            (
                {"type": "python", "name": "a", "file": "x.py", "module": "m"},
                "exactly one of",
            ),
            ({"type": "python", "name": "a", "file": "../x.py"}, "'..'"),
            ({"type": "python", "name": "a", "module": "not a module"}, "dotted"),
            (
                {
                    "type": "python",
                    "name": "a",
                    "git": {"url": "git@github.com:o/r"},
                    "path": "c.py",
                },
                "https://",
            ),
            (
                {"type": "python", "name": "a", "git": {"url": "https://x/r"}},
                "needs a path",
            ),
            (
                {"type": "python", "name": "a", "git": "elsewhere", "path": "c.py"},
                "'deployment'",
            ),
            (
                {
                    "type": "python",
                    "name": "a",
                    "git": {"url": "https://x/r", "commit": "zz"},
                    "path": "c.py",
                },
                "commit hash",
            ),
            (
                {"type": "python", "name": "a", "file": "x.py", "only": "price"},
                "list of check names",
            ),
            (
                {"type": "python", "name": "a", "file": "x.py", "exclude": ["1bad"]},
                "list of check names",
            ),
            (
                {"type": "python", "name": "a", "file": "x.py", "params": []},
                "keyed by check name",
            ),
            (
                {"type": "python", "name": "a", "file": "x.py", "params": {"price": 3}},
                "keyed by check name",
            ),
        ],
    )
    def test_refuses_what_the_job_could_not_run(self, entry, reason):
        with pytest.raises(ValueError, match=reason):
            _validate_entry(entry)
        with pytest.raises(SpecError, match=reason):
            _validate_spec(json.dumps([entry]))

    def test_source_kinds(self):
        assert _parse_source({"file": "Resources/a.py"}).kind == "file"
        assert _parse_source({"module": "a.b"}).kind == "module"
        assert _parse_source({"git": "deployment", "path": "a.py"}).kind == "deployment"
        git = _parse_source(
            {
                "git": {"url": "https://x/r", "ref": "main", "commit": "abc1234"},
                "path": "a.py",
            }
        )
        assert (git.kind, git.git_ref, git.git_commit) == ("git", "main", "abc1234")

    def test_building_a_spec_does_not_import_the_file(self, tmp_path):
        missing = tmp_path / "nowhere.py"
        [evaluator] = _evaluators_from_spec(
            [{"type": "python", "name": "x", "file": str(missing)}]
        )
        assert isinstance(evaluator, PythonSourceEvaluator)
        assert evaluator.name == "x"


# ── grading ─────────────────────────────────────────────────────────────────


def _file_evaluator(path: Path, name: str = "checks", **entry) -> PythonSourceEvaluator:
    sources = _CheckSources(dataset_api=object())
    # The file is local: stand in for the download with the path itself.
    sources._download = lambda path: path  # type: ignore[method-assign]
    [evaluator] = _evaluators_from_spec(
        [{"type": "python", "name": name, "file": str(path), **entry}],
        sources=sources,
    )
    return evaluator


class TestGrading:
    def test_every_declared_check_runs_under_its_own_name(self, check_file):
        evaluator = _file_evaluator(check_file, "pricing")
        trace = _trace({"name": "lookup_product", "result": '{"price": 19.99}'})
        rows = _rows([evaluator], trial=_trial("the price is 19.99"), trace=trace)
        assert set(rows) == DECLARED
        assert rows["price_matches_catalogue"].passed is True
        assert rows["answers_in_english"].passed is True
        for row in rows.values():
            assert row.evaluator_type == "python"
            assert row.assertions["row"] == "pricing"
            assert row.assertions["source"]["origin"].startswith("hopsfs ")
            assert row.assertions["source"]["sha256"]
        assert rows["price_matches_catalogue"].assertions["quoted"] == 19.99

    def test_params_are_per_check_over_the_defaults(self, check_file):
        evaluator = _file_evaluator(
            check_file, params={"price_matches_catalogue": {"tolerance": 5}}
        )
        trace = _trace({"name": "lookup_product", "result": '{"price": 15.00}'})
        rows = _rows([evaluator], trace=trace)
        assert rows["price_matches_catalogue"].passed is True

    def test_only_and_exclude_narrow_what_runs(self, check_file):
        rows = _rows([_file_evaluator(check_file, only=["answers_in_english"])])
        assert set(rows) == {"answers_in_english"}
        rows = _rows([_file_evaluator(check_file, exclude=["answers_in_english"])])
        assert set(rows) == {"price_matches_catalogue"}

    def test_ungradable_when_the_precondition_did_not_hold(self, check_file):
        rows = _rows([_file_evaluator(check_file)], trace=_trace())
        assert rows["price_matches_catalogue"].ungradable is True
        assert "never looked" in rows["price_matches_catalogue"].reason

    def test_no_trace_is_ungradable_only_for_the_checks_that_need_one(self, check_file):
        rows = _rows([_file_evaluator(check_file)], trial=_trial("the price"))
        assert rows["price_matches_catalogue"].ungradable is True
        assert rows["price_matches_catalogue"].passed is None
        assert rows["answers_in_english"].passed is True

    def test_a_raising_check_is_the_harness_failing(self, tmp_path):
        path = _write(
            tmp_path,
            "boom.py",
            """
            @check(needs_trace=False)
            def explodes(ctx):
                raise RuntimeError("boom")
            """,
        )
        rows = _rows([_file_evaluator(path)])
        assert rows["explodes"].ungradable is True
        assert "boom" in rows["explodes"].reason
        assert not rows["explodes"].assertions.get(SKIPPED)

    def test_a_hanging_check_times_out_without_a_verdict(self, tmp_path):
        path = _write(
            tmp_path,
            "slow.py",
            """
            import time
            @check(needs_trace=False, timeout_s=0.2)
            def slow(ctx):
                time.sleep(5)
                return True
            """,
        )
        started = time.monotonic()
        rows = _rows([_file_evaluator(path)])
        assert time.monotonic() - started < 3
        assert rows["slow"].ungradable is True
        assert "timed out" in rows["slow"].reason

    def test_a_module_that_blocks_at_import_is_bounded(self, tmp_path, monkeypatch):
        path = _write(
            tmp_path,
            "slow_import.py",
            """
            import time
            time.sleep(5)
            @check(needs_trace=False)
            def v(ctx):
                return True
            """,
        )
        monkeypatch.setattr(checks_module, "DEFAULT_TIMEOUT_S", 0.2)
        monkeypatch.setattr(checks_module, "IMPORT_LOCK_WAIT_S", 0.2)
        evaluator = _file_evaluator(path, "slow")
        started = time.monotonic()
        rows = _rows([evaluator])
        assert time.monotonic() - started < 3
        assert rows["slow"].assertions[SKIPPED] is True
        assert "importing the source took longer" in rows["slow"].reason

    def test_a_check_calling_sys_exit_is_the_check_failing(self, tmp_path):
        path = _write(
            tmp_path,
            "leaving.py",
            """
            import sys
            @check(needs_trace=False)
            def v(ctx):
                sys.exit(3)
            """,
        )
        rows = _rows([_file_evaluator(path)])
        assert rows["v"].ungradable is True
        assert "SystemExit" in rows["v"].reason

    def test_a_name_in_only_the_source_does_not_declare_is_skipped(self, check_file):
        evaluator = _file_evaluator(
            check_file, only=["no_such_check", "answers_in_english"]
        )
        for _ in range(2):
            rows = _rows([evaluator], trial=_trial("the x"))
            assert set(rows) == {"no_such_check", "answers_in_english"}
            assert rows["no_such_check"].assertions[SKIPPED] is True
            assert (
                "declares no check named 'no_such_check'"
                in rows["no_such_check"].reason
            )
            assert "price_matches_catalogue" in rows["no_such_check"].reason
            assert rows["answers_in_english"].passed is True

    def test_a_source_declaring_nothing_is_one_skipped_row(self, tmp_path):
        path = tmp_path / "nothing.py"
        path.write_text("def helper():\n    return 1\n")
        rows = _rows([_file_evaluator(path, "nothing")])
        assert set(rows) == {"nothing"}
        assert rows["nothing"].assertions[SKIPPED] is True
        assert "declares no @check" in rows["nothing"].reason

    def test_an_unreadable_file_is_skipped_not_failed(self, tmp_path):
        rows = _rows([_file_evaluator(tmp_path / "missing.py", "x")])
        assert rows["x"].assertions[SKIPPED] is True
        assert rows["x"].passed is None

    def test_no_sources_means_only_modules_load(self):
        [evaluator] = _evaluators_from_spec(
            [{"type": "python", "name": "x", "file": "Resources/a.py"}]
        )
        rows = _rows([evaluator])
        assert rows["x"].assertions[SKIPPED] is True
        assert "no way to read" in rows["x"].reason

    def test_module_source_imports_from_the_environment(self, tmp_path, monkeypatch):
        package = tmp_path / "customer_checks"
        package.mkdir()
        (package / "__init__.py").write_text(
            "from hopsworks_agents.eval.checks import check\n"
            "@check(needs_trace=False)\ndef short(ctx, limit: int = 20):\n"
            "    return len(ctx.answer) <= limit\n"
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        [evaluator] = _evaluators_from_spec(
            [{"type": "python", "name": "mod", "module": "customer_checks"}]
        )
        rows = _rows([evaluator], trial=_trial("ok"))
        assert rows["short"].passed is True
        assert (
            rows["short"]
            .assertions["source"]["origin"]
            .startswith("module customer_checks")
        )

    def test_a_check_that_reads_the_expectation_is_ungradable_without_one(
        self, tmp_path
    ):
        """Declared to read what only a task carries; on live traffic there is none."""
        path = _write(
            tmp_path,
            "expects.py",
            """
            @check(needs_trace=False, reads_expectation=True)
            def mentions(ctx):
                return ctx.expectation.lower() in ctx.answer.lower()
            """,
        )
        evaluator = _file_evaluator(path)
        # A task with nothing for it: the empty expectation is in every answer.
        rows = _rows([evaluator], task=_task(expectations={}), trial=_trial("anything"))
        assert rows["mentions"].ungradable is True
        assert "carries none" in rows["mentions"].reason
        rows = _rows(
            [evaluator],
            task=_task(expectations={"mentions": "any"}),
            trial=_trial("anything"),
        )
        assert rows["mentions"].passed is True

    def test_a_package_directory_loads_its_init(self, tmp_path):
        package = tmp_path / "checks"
        package.mkdir()
        (package / "helpers.py").write_text("def yes():\n    return True\n")
        (package / "__init__.py").write_text(
            "from hopsworks_agents.eval.checks import check\n"
            "from .helpers import yes\n"
            "@check(needs_trace=False)\ndef uses_helper(ctx):\n    return yes()\n"
        )
        rows = _rows([_file_evaluator(package)])
        assert rows["uses_helper"].passed is True


class TestReturnValues:
    @pytest.mark.parametrize(
        ("value", "passed", "score"),
        [
            (True, True, 1.0),
            (False, False, 0.0),
            (0.8, False, 0.8),
            (1.0, True, 1.0),
            (CheckResult(passed=False, reason="nope"), False, 0.0),
            (CheckResult(score=0.4), False, 0.4),
            ({"passed": True, "assertions": {"k": 1}}, True, 1.0),
        ],
    )
    def test_each_shape_becomes_a_row(self, value, passed, score):
        result = run_check(lambda ctx: value, answer="x", tools=[])
        assert (result.passed, result.score) == (passed, score)
        assert result.ungradable is False

    def test_pass_score_param_decides_a_float(self):
        result = run_check(lambda ctx, pass_score=0.5: 0.6, answer="x", tools=[])
        assert result.passed is True

    @pytest.mark.parametrize(
        "value",
        [float("nan"), float("inf"), CheckResult(score=float("nan"))],
    )
    def test_a_score_that_is_not_a_number_is_ungradable_not_perfect(self, value):
        result = run_check(lambda ctx: value, answer="x", tools=[])
        assert result.ungradable is True
        assert "not a finite number" in result.reason

    def test_sys_exit_from_run_check_is_a_row_not_an_exit(self):
        def leave(ctx):
            raise SystemExit(2)

        result = run_check(leave, answer="x", tools=[])
        assert result.ungradable is True

    @pytest.mark.parametrize("value", [None, "yes", object()])
    def test_nonsense_is_ungradable(self, value):
        result = run_check(lambda ctx: value, answer="x", tools=[])
        assert result.ungradable is True

    def test_ungradable_helper(self):
        result = run_check(lambda ctx: ungradable("no price"), answer="x", tools=[])
        assert result.ungradable is True
        assert result.reason == "no price"


# ── trying a check locally ──────────────────────────────────────────────────


class TestRunCheck:
    def test_synthetic_inputs(self, check_file):
        module, _held, _sub, _read = _import_path(str(check_file))
        result = run_check(
            module.price_matches_catalogue,
            answer="It costs 19.99",
            tools=[{"name": "lookup_product", "result": '{"price": 19.99}'}],
            tolerance=0.01,
        )
        assert result.passed is True
        assert result.evaluator_name == "price_matches_catalogue"

    def test_the_context_carries_everything(self):
        seen = {}

        def capture(ctx: CheckContext, **params):
            seen["ctx"] = ctx
            return True

        run_check(
            capture,
            answer="42",
            question="meaning?",
            expectation="42",
            transcript="user: meaning?\nagent: 42",
            tools=[ToolCall(name="think", status="ERROR")],
            subject="ada",
            name="meaning",
            depth=3,
        )
        ctx = seen["ctx"]
        assert ctx.question == "meaning?"
        assert ctx.answer == "42"
        assert ctx.expectation == "42"
        assert ctx.expectations == {"meaning": "42"}
        assert ctx.messages == ({"role": "user", "content": "meaning?"},)
        assert ctx.tool_names == ["think"]
        assert ctx.tools[0].failed is True
        assert ctx.task.subject == "ada"
        assert ctx.params == {"depth": 3}
        assert ctx.check_name == "meaning"
        assert ctx.answer_json() == 42

    def test_task_and_trial_details_reach_the_context(self):
        seen = {}

        def capture(ctx):
            seen["ctx"] = ctx
            return True

        run_check(
            capture,
            answer="x",
            tools=[],
            context={"tenant": "acme"},
            task={
                "task_id": "t9",
                "version": 2,
                "task_type": "multi_turn",
                "category": "refunds",
                "tags": ["a"],
            },
            trial={
                "trial_id": "r/t9/2/0",
                "run_id": "r",
                "trial_index": 0,
                "deployment_id": 7,
                "status": "FAILED",
                "latency_ms": 12.5,
                "trace_status": "RECEIVED",
            },
        )
        ctx = seen["ctx"]
        assert ctx.task.context == {"tenant": "acme"}
        assert (ctx.task.task_id, ctx.task.version, ctx.task.task_type) == (
            "t9",
            2,
            "multi_turn",
        )
        assert ctx.task.category == "refunds" and ctx.task.tags == ("a",)
        assert ctx.trial.trial_id == "r/t9/2/0" and ctx.trial.deployment_id == 7
        assert ctx.trial.status == "FAILED" and ctx.trial.latency_ms == 12.5
        assert ctx.trial.trace_status == "RECEIVED"

    def test_token_counts_reach_the_trial(self):
        seen = {}

        def capture(ctx):
            seen["ctx"] = ctx
            return True

        run_check(
            capture,
            answer="x",
            tools=[],
            trial={"input_tokens": 120, "output_tokens": "7", "session_id": "s1"},
        )
        assert (seen["ctx"].trial.input_tokens, seen["ctx"].trial.output_tokens) == (
            120,
            7,
        )

    def test_the_job_trace_dict_is_accepted(self):
        result = run_check(
            lambda ctx: ctx.tool_names == ["a", "b"],
            answer="x",
            trace=_trace({"name": "a"}, {"name": "b"}),
        )
        assert result.passed is True

    def test_an_sdk_trace_is_read_like_the_job_reads_it(self):
        from hopsworks_agents.eval.sdk.models import Trace

        detail = {
            "spans": [
                {
                    "spanId": "root",
                    "traceId": "t1",
                    "startTimeNs": 1,
                    "endTimeNs": 5,
                    "messages": json.dumps(
                        [
                            {"role": "user", "content": "price of X?"},
                            {"role": "assistant", "content": "X costs 9"},
                        ]
                    ),
                },
                {
                    "spanId": "tool1",
                    "traceId": "t1",
                    "parentSpanId": "root",
                    "name": "lookup_product",
                    "startTimeNs": 2,
                    "endTimeNs": 3,
                    "statusCode": "OK",
                },
            ],
            "spanAttributes": [
                {
                    "spanId": "tool1",
                    "attrKey": "openinference.span.kind",
                    "attrValue": "TOOL",
                },
                {
                    "spanId": "tool1",
                    "attrKey": "output.value",
                    "attrValue": '{"price": 9}',
                },
            ],
        }
        trace = Trace.from_api(detail)
        seen = {}

        def capture(ctx):
            seen["ctx"] = ctx
            return True

        run_check(capture, trace=trace)
        ctx = seen["ctx"]
        assert ctx.question == "price of X?"
        assert ctx.answer == "X costs 9"
        assert ctx.tool_names == ["lookup_product"]
        assert ctx.tools[0].result_json == {"price": 9}

    def test_a_check_that_needs_a_trace_and_got_none(self):
        @check(needs_trace=True)
        def needs(ctx):
            return True

        assert run_check(needs, answer="x").ungradable is True
        assert run_check(needs, answer="x", tools=[]).passed is True

    def test_a_plain_lambda_is_fine(self):
        assert run_check(lambda ctx: ctx.answer == "x", answer="x", tools=[]).passed


# ── sources: files, git and snapshots ───────────────────────────────────────


class _FakeDatasetApi:
    """HopsFS as a dict of path -> text, with the calls the resolver makes."""

    def __init__(self, files: dict[str, str] | None = None):
        self.files = dict(files or {})
        self.uploaded: list[tuple[str, str]] = []

    def _get(self, path):
        path = "/" + path.lstrip("/")
        if path in self.files:
            return {
                "attributes": {
                    "path": path,
                    "dir": False,
                    "size": len(self.files[path]),
                }
            }
        if any(f.startswith(path.rstrip("/") + "/") for f in self.files):
            return {"attributes": {"path": path, "dir": True, "size": 0}}
        raise RuntimeError(f"{path} not found")

    def list(self, path, limit=1000):  # noqa: ARG002
        """Children as ``DatasetApi.list`` returns them: project-relative, with a leading slash."""
        prefix = "/" + path.strip("/") + "/"
        children = set()
        for f in self.files:
            if f.startswith(prefix):
                rest = f[len(prefix) :]
                child = prefix + rest.split("/", 1)[0]
                parts = child.split("/")
                if child.startswith("/Projects/") and len(parts) > 3:
                    child = "/" + "/".join(parts[3:])
                children.add(child)
        return sorted(children)

    def download(self, path, local, overwrite=True):  # noqa: ARG002
        os.makedirs(os.path.dirname(local), exist_ok=True)
        Path(local).write_bytes(self.files["/" + path.lstrip("/")].encode())
        return local

    def mkdir(self, path):
        return path

    def move(self, source, destination, overwrite=False):
        source = "/" + source.strip("/")
        destination = "/" + destination.strip("/")
        exists = any(
            f == destination or f.startswith(destination + "/") for f in self.files
        )
        if exists and not overwrite:
            raise RuntimeError(f"{destination} already exists")
        for f in list(self.files):
            if f == source or f.startswith(source + "/"):
                self.files[destination + f[len(source) :]] = self.files.pop(f)

    def remove(self, path):
        path = "/" + path.strip("/")
        for f in list(self.files):
            if f == path or f.startswith(path + "/"):
                del self.files[f]

    def upload(self, local, target, overwrite=True):  # noqa: ARG002
        local_path = Path(local)
        target = "/" + target.strip("/")
        if local_path.is_dir():
            for file in local_path.rglob("*"):
                if file.is_file():
                    rel = file.relative_to(local_path.parent).as_posix()
                    self.files[f"{target}/{rel}"] = file.read_text()
        else:
            self.files[f"{target}/{local_path.name}"] = local_path.read_text()
        self.uploaded.append((str(local), target))
        return target


VERDICT_FILE = (
    "from hopsworks_agents.eval.checks import check\n"
    "@check(needs_trace=False)\ndef verdict(ctx):\n    return {verdict}\n"
)


def _row(**where):
    return {"type": "python", "name": "checks", **where}


class TestSources:
    def test_a_relative_file_is_the_projects_and_is_downloaded(self, tmp_path):
        api = _FakeDatasetApi(
            {
                "/Projects/p/Resources/agent_evals/checks/pricing.py": textwrap.dedent(
                    CHECK_FILE
                )
            }
        )
        sources = _CheckSources(
            dataset_api=api, project_name="p", workdir=str(tmp_path)
        )
        [evaluator] = _evaluators_from_spec(
            [_row(file="Resources/agent_evals/checks/pricing.py")], sources=sources
        )
        rows = _rows([evaluator], trial=_trial("the x"))
        assert rows["answers_in_english"].passed is True
        assert rows["answers_in_english"].assertions["source"]["origin"] == (
            "hopsfs /Projects/p/Resources/agent_evals/checks/pricing.py"
        )

    def test_a_package_directory_is_downloaded_whole(self, tmp_path):
        api = _FakeDatasetApi(
            {
                "/Projects/p/Resources/checks/__init__.py": (
                    "from hopsworks_agents.eval.checks import check\n"
                    "from .helpers import yes\n"
                    "@check(needs_trace=False)\ndef uses_helper(ctx):\n    return yes()\n"
                ),
                "/Projects/p/Resources/checks/helpers.py": "def yes():\n    return True\n",
            }
        )
        sources = _CheckSources(
            dataset_api=api, project_name="p", workdir=str(tmp_path)
        )
        [evaluator] = _evaluators_from_spec(
            [_row(file="Resources/checks")], sources=sources
        )
        assert _rows([evaluator])["uses_helper"].passed is True

    def test_a_file_source_is_read_live_and_never_pinned(self, tmp_path):
        """Published or not, a project file is read as it is now, and the row says which text."""
        snapshot_dir = "/Projects/p/Resources/agent_evals/suites/s1/v2/checks"
        api = _FakeDatasetApi(
            {"/Projects/p/Resources/checks/c.py": VERDICT_FILE.format(verdict="False")}
        )
        sources = _CheckSources(
            dataset_api=api,
            project_name="p",
            snapshot_dir=snapshot_dir,
            workdir=str(tmp_path),
        )
        [live] = _evaluators_from_spec(
            [{"type": "python", "name": "live", "file": "Resources/checks/c.py"}],
            sources=sources,
        )
        row = _rows([live])["verdict"]
        assert row.passed is False
        source = row.assertions["source"]
        assert source["pinned"] is False
        assert source["origin"] == "hopsfs /Projects/p/Resources/checks/c.py"
        # The hash is of what the import read: each file by its relative path.
        assert source["files"] == ["c.py"]
        assert (
            source["sha256"]
            == hashlib.sha256(
                b"c.py\0" + VERDICT_FILE.format(verdict="False").encode()
            ).hexdigest()
        )
        # Nothing was written beside the suite's tasks for it.
        assert not any(f.startswith(snapshot_dir) for f in api.files)

    def _fake_clone(self, tmp_path):
        def clone(url, ref, commit, into):
            root = Path(into) / "repo"
            (root / "evaluation" / "checks").mkdir(parents=True)
            (root / "evaluation" / "checks" / "pricing.py").write_text(
                textwrap.dedent(CHECK_FILE)
            )
            (root / ".git").mkdir()
            (root / ".git" / "HEAD").write_text("abc123def\n")
            clone.calls.append((url, ref, commit))
            return str(root)

        clone.calls = []
        return clone

    def _git_row(self, url="https://github.com/o/r.git"):
        return _row(
            git={"url": url, "ref": "main"}, path="evaluation/checks/pricing.py"
        )

    def test_a_git_source_is_cloned_once_and_the_commit_recorded(self, tmp_path):
        clone = self._fake_clone(tmp_path)
        sources = _CheckSources(workdir=str(tmp_path), clone=clone)
        a, b = _evaluators_from_spec(
            [
                {**self._git_row(), "name": "a"},
                {**self._git_row(), "name": "b", "only": ["answers_in_english"]},
            ],
            sources=sources,
        )
        trace = _trace({"name": "lookup_product", "result": '{"price": 19.99}'})
        results = _run_evaluators([a, b], _task(), _trial("the 19.99"), trace)
        assert [r.evaluator_name for r in results] == [
            "price_matches_catalogue",
            "answers_in_english",
            "b/answers_in_english",
        ]
        assert all(r.passed for r in results)
        assert clone.calls == [("https://github.com/o/r.git", "main", "")]
        assert results[0].assertions["source"]["commit"] == "abc123def"
        assert results[0].assertions["source"]["pinned"] is False

    def test_a_published_suite_pins_a_git_source_on_first_run(self, tmp_path):
        clone = self._fake_clone(tmp_path)
        api = _FakeDatasetApi()
        snapshot_dir = "/Projects/p/Resources/agent_evals/suites/s1/v1/checks"
        sources = _CheckSources(
            dataset_api=api,
            project_name="p",
            snapshot_dir=snapshot_dir,
            workdir=str(tmp_path),
            clone=clone,
        )
        [evaluator] = _evaluators_from_spec([self._git_row()], sources=sources)
        rows = _rows([evaluator], trial=_trial("the x"))
        assert rows["answers_in_english"].passed is True
        assert rows["answers_in_english"].assertions["source"]["pinned"] is True

        # Frozen under a name that is the whole source identity, with the tree
        # around the file and a manifest inside, and no staging left behind.
        [name] = {
            f[len(snapshot_dir) + 1 :].split("/", 1)[0]
            for f in api.files
            if f.startswith(snapshot_dir + "/")
        }
        assert re.fullmatch(r"[0-9a-f]{12}-pricing\.py", name), name
        assert f"{snapshot_dir}/{name}/tree/evaluation/checks/pricing.py" in api.files
        manifest = json.loads(api.files[f"{snapshot_dir}/{name}/manifest.json"])
        assert manifest["commit"] == "abc123def"
        assert manifest["url"] == "https://github.com/o/r.git"
        assert manifest["path"] == "evaluation/checks/pricing.py"
        assert manifest["basename"] == "pricing.py"
        assert manifest["tree"] is True
        assert not any(".staging-" in f for f in api.files)

        # The next run reads the snapshot and never clones.
        later = _CheckSources(
            dataset_api=api,
            project_name="p",
            snapshot_dir=snapshot_dir,
            workdir=str(tmp_path / "later"),
            clone=self._fake_clone(tmp_path),
        )
        [again] = _evaluators_from_spec([self._git_row()], sources=later)
        rows = _rows([again], trial=_trial("the x"))
        source = rows["answers_in_english"].assertions["source"]
        assert source["pinned"] is True
        assert source["commit"] == "abc123def"
        assert source["origin"].startswith("snapshot ")
        assert later._clone.calls == []

    def test_a_pinned_source_keeps_the_helpers_it_imports_from_the_repository(
        self, tmp_path
    ):
        """The tree is frozen, not the one file, so a helper elsewhere still imports."""

        def clone(url, ref, commit, into):  # noqa: ARG001
            root = Path(into) / "repo"
            (root / "evaluation" / "checks").mkdir(parents=True)
            (root / "shared").mkdir()
            (root / "shared" / "__init__.py").write_text("")
            (root / "shared" / "money.py").write_text("def ok():\n    return True\n")
            (root / "evaluation" / "checks" / "c.py").write_text(
                "from hopsworks_agents.eval.checks import check\n"
                "from shared.money import ok\n"
                "@check(needs_trace=False)\ndef uses_shared(ctx):\n    return ok()\n"
            )
            return str(root)

        api = _FakeDatasetApi()
        snapshot_dir = "/Projects/p/Resources/agent_evals/suites/s1/v1/checks"
        row = _row(
            git={"url": "https://x/r.git", "ref": "main"}, path="evaluation/checks/c.py"
        )
        first = _CheckSources(
            dataset_api=api,
            project_name="p",
            snapshot_dir=snapshot_dir,
            workdir=str(tmp_path / "a"),
            clone=clone,
        )
        assert (
            _rows(_evaluators_from_spec([row], sources=first))["uses_shared"].passed
            is True
        )

        def no_clone(*args):  # noqa: ARG001
            raise AssertionError("the snapshot should have been read instead")

        later = _CheckSources(
            dataset_api=api,
            project_name="p",
            snapshot_dir=snapshot_dir,
            workdir=str(tmp_path / "b"),
            clone=no_clone,
        )
        rows = _rows(_evaluators_from_spec([row], sources=later))
        assert rows["uses_shared"].passed is True, rows["uses_shared"].reason
        assert rows["uses_shared"].assertions["source"]["pinned"] is True

    def test_two_first_runs_pin_once_and_the_loser_grades_with_the_winner(
        self, tmp_path
    ):
        """Two runs clone a branch that moved between them; one copy is frozen, both use it."""

        def clone_returning(verdict):
            def clone(url, ref, commit, into):  # noqa: ARG001
                root = Path(into) / "repo"
                (root / "c").mkdir(parents=True)
                (root / "c" / "k.py").write_text(VERDICT_FILE.format(verdict=verdict))
                return str(root)

            return clone

        api = _FakeDatasetApi()
        snapshot_dir = "/Projects/p/Resources/agent_evals/suites/s1/v1/checks"
        row = _row(git={"url": "https://x/r.git", "ref": "main"}, path="c/k.py")
        a = _CheckSources(
            dataset_api=api,
            project_name="p",
            snapshot_dir=snapshot_dir,
            workdir=str(tmp_path / "a"),
            clone=clone_returning("True"),
        )
        b = _CheckSources(
            dataset_api=api,
            project_name="p",
            snapshot_dir=snapshot_dir,
            workdir=str(tmp_path / "b"),
            clone=clone_returning("False"),
        )
        # Both fetch before either imports: neither saw a snapshot.
        [source] = [e.source for e in _evaluators_from_spec([row], sources=a)]
        fetched_a = a._fetch_local(source)
        fetched_b = b._fetch_local(source)
        assert fetched_a.pinned and fetched_b.pinned
        result_a = _rows(_evaluators_from_spec([row], sources=a))["verdict"]
        result_b = _rows(_evaluators_from_spec([row], sources=b))["verdict"]
        # a staged first and won; b's rename was refused and b loaded a's copy.
        assert (result_a.passed, result_b.passed) == (True, True)
        assert len([f for f in api.files if f.endswith("/manifest.json")]) == 1

    def test_the_deployments_own_repository(self, tmp_path):
        clone = self._fake_clone(tmp_path)
        api = _FakeDatasetApi()
        sources = _CheckSources(
            dataset_api=api,
            project_name="p",
            serving_view=lambda: {
                "name": "support",
                "gitUrl": "https://github.com/o/agent.git",
                "gitBranch": "main",
                "gitCurrentCommit": "fedcba9",
            },
            snapshot_dir="/Projects/p/Resources/agent_evals/suites/s1/v1/checks",
            workdir=str(tmp_path),
            clone=clone,
        )
        [evaluator] = _evaluators_from_spec(
            [_row(git="deployment", path="evaluation/checks/pricing.py")],
            sources=sources,
        )
        rows = _rows([evaluator], trial=_trial("the x"))
        assert rows["answers_in_english"].passed is True
        assert clone.calls == [("https://github.com/o/agent.git", "main", "fedcba9")]
        source = rows["answers_in_english"].assertions["source"]
        assert source["commit"] == "fedcba9"
        # Never pinned: the checks move with the agent.
        assert source["pinned"] is False
        assert api.uploaded == []

    def test_a_deployment_without_a_repository_is_skipped(self, tmp_path):
        sources = _CheckSources(
            serving_view=lambda: {"name": "local-agent", "modelPath": "/Projects/p/x"},
            workdir=str(tmp_path),
        )
        [evaluator] = _evaluators_from_spec(
            [{"type": "python", "name": "x", "git": "deployment", "path": "c.py"}],
            sources=sources,
        )
        rows = _rows([evaluator])
        assert rows["x"].assertions[SKIPPED] is True
        assert "not git-backed" in rows["x"].reason

    def test_a_failed_clone_names_no_credential(self, tmp_path):
        def clone(url, ref, commit, into):  # noqa: ARG001
            raise RuntimeError("fatal: could not read from https://u:secret@x/r")

        sources = _CheckSources(workdir=str(tmp_path), clone=clone)
        [evaluator] = _evaluators_from_spec(
            [
                {
                    "type": "python",
                    "name": "x",
                    "git": {"url": "https://x/r", "ref": "main"},
                    "path": "c.py",
                }
            ],
            sources=sources,
        )
        rows = _rows([evaluator])
        assert rows["x"].assertions[SKIPPED] is True
        assert "secret" not in rows["x"].reason

    @staticmethod
    def _repo_with_helper(verdict: str):
        """A clone whose entry file imports a sibling `helpers` module."""

        def clone(url, ref, commit, into):  # noqa: ARG001
            root = Path(into) / "repo"
            root.mkdir(parents=True)
            (root / "helpers.py").write_text(f"def verdict():\n    return {verdict}\n")
            (root / "checks.py").write_text(
                "from hopsworks_agents.eval.checks import check\n"
                "from helpers import verdict\n"
                "@check(needs_trace=False)\ndef v(ctx):\n    return verdict()\n"
            )
            return str(root)

        return clone

    def _two_repos(self, tmp_path):
        """Rows a and b, each a repository with its own `helpers`."""
        clones = {
            "https://x/a.git": self._repo_with_helper("True"),
            "https://x/b.git": self._repo_with_helper("False"),
        }

        def clone(url, ref, commit, into):
            return clones[url](url, ref, commit, into)

        sources = _CheckSources(workdir=str(tmp_path), clone=clone)
        return _evaluators_from_spec(
            [
                {
                    "type": "python",
                    "name": "a",
                    "git": {"url": "https://x/a.git"},
                    "path": "checks.py",
                },
                {
                    "type": "python",
                    "name": "b",
                    "git": {"url": "https://x/b.git"},
                    "path": "checks.py",
                },
            ],
            sources=sources,
        )

    def test_helpers_of_the_same_name_do_not_leak_between_sources(self, tmp_path):
        """A second source's `helpers` must not be the first's, cached in sys.modules."""
        a, b = self._two_repos(tmp_path)
        results = _run_evaluators([a, b], _task(), _trial(), None)
        assert [(r.evaluator_name, r.assertions["row"], r.passed) for r in results] == [
            ("v", "a", True),
            ("b/v", "b", False),
        ]
        assert "helpers" not in sys.modules
        assert not any(str(tmp_path) in entry for entry in sys.path)

    def test_a_helper_beside_a_file_imports_with_or_without_the_mount(self, tmp_path):
        """The job reads the file off the HopsFS mount; a download fetches the siblings too."""
        (tmp_path / "helpers.py").write_text("def verdict():\n    return True\n")
        (tmp_path / "checks.py").write_text(
            "from hopsworks_agents.eval.checks import check\n"
            "from helpers import verdict\n"
            "@check(needs_trace=False)\ndef v(ctx):\n    return verdict()\n"
        )
        # In place, as off the mount.
        rows = _rows([_file_evaluator(tmp_path / "checks.py", "mounted")])
        assert rows["v"].passed is True
        assert str(tmp_path) not in sys.path
        # Downloaded: the entry and the .py files beside it, nothing else.
        api = _FakeDatasetApi(
            {
                "/Projects/p/Resources/c/checks.py": (
                    tmp_path / "checks.py"
                ).read_text(),
                "/Projects/p/Resources/c/helpers.py": (
                    tmp_path / "helpers.py"
                ).read_text(),
                "/Projects/p/Resources/c/notes.txt": "not code",
                "/Projects/p/Resources/c/sub/deep.py": "x = 1\n",
            }
        )
        sources = _CheckSources(
            dataset_api=api, project_name="p", workdir=str(tmp_path / "w")
        )
        [row] = _evaluators_from_spec(
            [{"type": "python", "name": "fetched", "file": "Resources/c/checks.py"}],
            sources=sources,
        )
        assert _rows([row])["v"].passed is True
        fetched = {
            os.path.relpath(os.path.join(d, f), tmp_path / "w")
            for d, _, files in os.walk(tmp_path / "w")
            for f in files
            if f.endswith(".py")
        }
        assert {os.path.basename(f) for f in fetched} == {"checks.py", "helpers.py"}

    def test_a_package_imports_its_own_modules_and_declares_their_checks(
        self, tmp_path
    ):
        """What `__init__` imports from the package is the package's, checks included."""
        pkg = tmp_path / "pkg"
        pkg.mkdir()
        (pkg / "helpers.py").write_text("def verdict():\n    return True\n")
        (pkg / "pricing.py").write_text(
            "from hopsworks_agents.eval.checks import check\n"
            "from .helpers import verdict\n"
            "@check(needs_trace=False)\ndef in_pricing(ctx):\n    return verdict()\n"
        )
        (pkg / "__init__.py").write_text(
            "from hopsworks_agents.eval.checks import check\n"
            "from . import pricing\n"
            "@check(needs_trace=False)\ndef in_init(ctx):\n    return True\n"
        )
        rows = _rows([_file_evaluator(pkg, "pkg")])
        assert {name: r.passed for name, r in rows.items()} == {
            "in_init": True,
            "in_pricing": True,
        }

    def test_two_rows_sharing_a_check_name_never_share_a_result_id(self, tmp_path):
        """Stored results are keyed by trial and name; the second row's name says which row."""
        from hopsworks_agents.eval.checks import _preload

        for name in ("x", "y"):
            (tmp_path / f"{name}.py").write_text(VERDICT_FILE.format(verdict="True"))
        sources = _CheckSources(dataset_api=object())
        sources._download = lambda path: path  # type: ignore[method-assign]
        rows = _evaluators_from_spec(
            [
                {"type": "python", "name": "x", "file": str(tmp_path / "x.py")},
                {"type": "python", "name": "y", "file": str(tmp_path / "y.py")},
            ],
            sources=sources,
        )
        # Preloaded in spec order, so the first row keeps the bare name whatever
        # trial thread grades first.
        _preload(rows)
        results = _run_evaluators(list(reversed(rows)), _task(), _trial(), None)
        assert sorted(r.evaluator_name for r in results) == ["verdict", "y/verdict"]
        assert len({f"t/{r.evaluator_name}" for r in results}) == 2

    def test_concurrent_trials_do_not_hand_one_source_anothers_helper(self, tmp_path):
        """Importing edits sys.path and sys.modules; trials run on threads."""
        import concurrent.futures

        verdicts = set()
        for round_ in range(3):
            a, b = self._two_repos(tmp_path / str(round_))
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                futures = [
                    pool.submit(_run_evaluators, [a, b], _task(), _trial(), None)
                    for _ in range(16)
                ]
                for future in futures:
                    verdicts.add(
                        tuple((r.assertions["row"], r.passed) for r in future.result())
                    )
        assert verdicts == {(("a", True), ("b", False))}

    def test_a_blocked_import_does_not_stall_the_rows_after_it(
        self, tmp_path, monkeypatch
    ):
        """good, blocked, good again: the third row is served, not deadlocked."""
        from hopsworks_agents.eval.checks import _preload

        good = tmp_path / "good.py"
        good.write_text(VERDICT_FILE.format(verdict="True"))
        blocked = _write(
            tmp_path,
            "blocked.py",
            """
            import time
            time.sleep(2)
            @check(needs_trace=False)
            def v(ctx):
                return True
            """,
        )
        # Short on purpose: the stuck thread keeps the process-wide import lock
        # until its sleep ends, and the tests after this one would wait for it.
        monkeypatch.setattr(checks_module, "DEFAULT_TIMEOUT_S", 0.3)
        monkeypatch.setattr(checks_module, "IMPORT_LOCK_WAIT_S", 0.3)
        sources = _CheckSources(dataset_api=object())
        sources._download = lambda path: path  # type: ignore[method-assign]
        rows = _evaluators_from_spec(
            [
                {"type": "python", "name": "first", "file": str(good)},
                {"type": "python", "name": "stuck", "file": str(blocked)},
                {"type": "python", "name": "again", "file": str(good)},
            ],
            sources=sources,
        )
        started = time.monotonic()
        _preload(rows)
        results = _run_evaluators(rows, _task(), _trial(), None)
        assert time.monotonic() - started < 5
        by_row = {r.assertions.get("row"): r for r in results}
        assert by_row["first"].passed is True
        assert by_row["stuck"].assertions[SKIPPED] is True
        assert "took longer" in by_row["stuck"].reason
        assert by_row["again"].passed is True

    def test_a_declared_check_never_writes_over_a_built_ins_row(self, tmp_path):
        (tmp_path / "c.py").write_text(VERDICT_FILE.format(verdict="False"))
        sources = _CheckSources(dataset_api=object())
        sources._download = lambda path: path  # type: ignore[method-assign]
        built_in, python = _evaluators_from_spec(
            [
                {"type": "exact_match", "name": "verdict"},
                {"type": "python", "name": "x", "file": str(tmp_path / "c.py")},
            ],
            sources=sources,
        )
        results = _run_evaluators(
            [built_in, python],
            _task(expectations={"verdict": "ok"}),
            _trial("ok"),
            None,
        )
        assert [(r.evaluator_name, r.passed) for r in results] == [
            ("verdict", True),
            ("x/verdict", False),
        ]

    def test_an_explicit_empty_only_selects_nothing(self, tmp_path):
        (tmp_path / "c.py").write_text(VERDICT_FILE.format(verdict="True"))
        rows = _rows([_file_evaluator(tmp_path / "c.py", "nothing", only=[])])
        assert set(rows) == {"nothing"}
        assert rows["nothing"].ungradable is True
        assert not rows["nothing"].assertions.get(SKIPPED)
        assert "no check selected" in rows["nothing"].reason

    def test_a_snapshot_is_named_by_the_commit_a_row_pins(self, tmp_path):
        sources = _CheckSources(workdir=str(tmp_path))
        at_one = _parse_source(
            {
                "git": {"url": "https://x/r", "ref": "main", "commit": "aaaaaaa"},
                "path": "c.py",
            }
        )
        at_two = _parse_source(
            {
                "git": {"url": "https://x/r", "ref": "main", "commit": "bbbbbbb"},
                "path": "c.py",
            }
        )
        tip = _parse_source(
            {"git": {"url": "https://x/r", "ref": "main"}, "path": "c.py"}
        )
        names = {sources._snapshot_key(s) for s in (at_one, at_two, tip)}
        assert len(names) == 3
        assert all(n.endswith("-c.py") for n in names)

    def test_a_file_is_this_projects(self):
        """Absolute paths are accepted under this project only; the rule is not ee's alone."""
        sources = _CheckSources(project_name="p")
        assert sources._absolute("Resources/x.py") == "/Projects/p/Resources/x.py"
        assert sources._absolute("/Projects/p/Resources/x.py") == (
            "/Projects/p/Resources/x.py"
        )
        for other in (
            "/Projects/q/Resources/x.py",
            "/Projects/p/../q/x.py",
            "/apps/shared/x.py",
            "C:\\checks\\pricing.py",
            "D:/checks/pricing.py",
        ):
            with pytest.raises(ValueError, match="not .* this project"):
                sources._absolute(other)
        # Without a project (a local run), a drive path is absolute, not relative.
        local = _CheckSources()
        assert local._absolute("C:\\checks\\pricing.py") == "C:\\checks\\pricing.py"
        assert local._absolute("D:/checks/pricing.py") == "D:/checks/pricing.py"


class TestRunJobWiring:
    def test_check_sources_pins_only_a_published_suite(self):
        from hopsworks_agents.eval.run_job import _check_sources

        class Session:
            def __init__(self, status):
                self.status = status

            def get(self, url, params=None, timeout=None):  # noqa: ARG002
                status = self.status

                class Response:
                    def json(self):
                        return {"status": status, "version": 3}

                return Response()

        class Project:
            id = 7
            name = "p"

            def get_dataset_api(self):
                return object()

        run = {
            "deploymentId": 9,
            "suiteId": "s1",
            "suiteVersion": 3,
            "runType": "SUITE",
        }
        published = _check_sources(
            Session("PUBLISHED"), "base", "https://h", Project(), run
        )
        draft = _check_sources(Session("DRAFT"), "base", "https://h", Project(), run)
        sample = _check_sources(
            Session("PUBLISHED"),
            "base",
            "https://h",
            Project(),
            {**run, "runType": "ONLINE_SAMPLE"},
        )
        assert published._snapshot_dir == (
            "/Projects/p/Resources/agent_evals/suites/s1/v3/checks"
        )
        assert draft._snapshot_dir is None
        assert sample._snapshot_dir is None
        for sources in (published, draft, sample):
            sources._close()

    def test_module_constants(self):
        assert checks_module.TYPE == "python"
        from hopsworks_agents.eval.evaluator_spec import SPEC_TYPES

        assert "python" in SPEC_TYPES


# ── review round 4 ──────────────────────────────────────────────────────────


class TestRowsAreWritable:
    """What a check returns ends up in a JSON column; it must get there."""

    def test_assertions_that_are_not_json_are_written_as_text(self):
        from datetime import datetime, timezone
        from decimal import Decimal

        when = datetime(2026, 11, 4, tzinfo=timezone.utc)

        @check(needs_trace=False)
        def odd(ctx):
            return CheckResult(
                passed=True,
                reason=Decimal("1.5"),  # type: ignore[arg-type]
                assertions={
                    "expected": Decimal("19.99"),
                    "when": when,
                    "ids": {2, 1},
                    "pair": (1, 2),
                    "raw": b"bytes",
                },
            )

        result = run_check(odd, answer="x")
        json.dumps(result.assertions)
        assert result.reason == "1.5"
        assert result.assertions["expected"] == "19.99"
        assert result.assertions["when"] == when.isoformat()
        assert sorted(result.assertions["ids"]) == [1, 2]
        assert result.assertions["pair"] == [1, 2]
        assert result.assertions["raw"] == "bytes"

    def test_numpy_scalars_are_verdicts(self):
        numpy = pytest.importorskip("numpy")

        @check(needs_trace=False)
        def yes(ctx):
            return numpy.bool_(True)

        @check(needs_trace=False)
        def scored(ctx):
            return CheckResult(
                score=numpy.float64(0.5), assertions={"n": numpy.int64(3)}
            )

        assert run_check(yes, answer="x").passed is True
        row = run_check(scored, answer="x", pass_score=0.5)
        assert (row.passed, row.score) == (True, 0.5)
        assert row.assertions == {"n": 3}
        json.dumps(row.assertions)

    def test_a_malformed_result_fails_only_its_own_check(self, tmp_path):
        """Reading the return value is the check's failure, not its siblings'."""
        path = _write(
            tmp_path,
            "pair.py",
            """
            class Mute:
                def __str__(self):
                    raise RuntimeError("no text for this")

            @check(needs_trace=False)
            def circular(ctx):
                return CheckResult(passed=True, reason=Mute())

            @check(needs_trace=False)
            def fine(ctx):
                return True
            """,
        )
        rows = _rows([_file_evaluator(path, "pair")])
        assert rows["fine"].passed is True
        assert rows["circular"].ungradable is True
        assert "check raised" in rows["circular"].reason

    def test_pass_score_is_read_here_not_handed_to_the_check(self):
        @check(needs_trace=False)
        def scored(ctx):
            return 0.6

        @check(needs_trace=False)
        def takes_it(ctx, pass_score: float = 0.9):
            return CheckResult(score=0.6, assertions={"seen": pass_score})

        assert run_check(scored, answer="x", pass_score=0.5).passed is True
        assert run_check(scored, answer="x", pass_score=0.7).passed is False
        row = run_check(takes_it, answer="x", pass_score=0.5)
        assert row.passed is True and row.assertions == {"seen": 0.5}

    def test_pass_score_in_a_rows_params_decides_a_float(self, tmp_path):
        path = _write(
            tmp_path,
            "scored.py",
            """
            @check(needs_trace=False)
            def scored(ctx):
                return 0.6
            """,
        )
        rows = _rows(
            [_file_evaluator(path, "s", params={"scored": {"pass_score": 0.5}})]
        )
        assert rows["scored"].passed is True and rows["scored"].score == 0.6


class TestACheckCannotHurtTheRun:
    def test_a_hanging_check_is_called_once_per_run(self, tmp_path):
        path = _write(
            tmp_path,
            "hang.py",
            """
            import time
            CALLS = []

            @check(needs_trace=False, timeout_s=0.2)
            def hangs(ctx):
                CALLS.append(1)
                time.sleep(3)
                return True

            @check(needs_trace=False)
            def quick(ctx):
                return True
            """,
        )
        evaluator = _file_evaluator(path, "h")
        first = _rows([evaluator])
        second = _rows([evaluator])
        assert "timed out after 0.2s" in first["hangs"].reason
        assert "not called again" in second["hangs"].reason
        assert second["quick"].passed is True
        assert evaluator._loaded.module.CALLS == [1]

    def test_a_check_cannot_reorder_the_runners_trace(self):
        @check
        def meddles(ctx):
            ctx.trace["tool_calls"].reverse()
            ctx.trace["tool_names"].clear()
            return True

        trace = _trace({"name": "a"}, {"name": "b"})
        assert run_check(meddles, answer="x", trace=trace).passed is True
        assert [c["name"] for c in trace["tool_calls"]] == ["a", "b"]
        assert trace["tool_names"] == ["a", "b"]

    def test_run_check_honours_reads_expectation(self):
        @check(needs_trace=False, reads_expectation=True)
        def agrees(ctx):
            return ctx.expectation in ctx.answer

        without = run_check(agrees, answer="yes")
        assert without.ungradable is True
        assert "carries none" in without.reason
        assert run_check(agrees, answer="yes", expectation="yes").passed is True

    def test_a_missing_only_name_never_writes_over_a_built_ins_row(self, tmp_path):
        (tmp_path / "c.py").write_text(VERDICT_FILE.format(verdict="True"))
        sources = _CheckSources(dataset_api=object())
        sources._download = lambda path: path  # type: ignore[method-assign]
        rows = _evaluators_from_spec(
            [
                {"type": "exact_match", "name": "contains"},
                {"type": "regex", "name": "x", "pattern": "^the"},
                {
                    "type": "python",
                    "name": "p",
                    "file": str(tmp_path / "c.py"),
                    "only": ["contains"],
                },
                {"type": "python", "name": "x", "file": str(tmp_path / "missing.py")},
            ],
            sources=sources,
        )
        results = _run_evaluators(rows, _task(), _trial(), None)
        names = [r.evaluator_name for r in results]
        assert names == ["contains", "x", "p/contains", "x/x"]
        assert results[2].assertions[SKIPPED] is True
        assert results[3].assertions[SKIPPED] is True
        assert len({f"t/{n}" for n in names}) == 4


class TestRefusals:
    def test_retired_row_keys_are_refused(self):
        for key in (
            "check",
            "function",
            "timeout_s",
            "needs_trace",
            "reads_expectation",
        ):
            with pytest.raises(ValueError, match=f"{key} is not a row key"):
                _validate_entry({"type": "python", "file": "c.py", key: "price"})

    def test_a_url_with_credentials_is_refused(self):
        for url in (
            "https://user:tok@github.com/o/r.git",
            "https://github.com/o/r.git?access_token=tok",
            "https://github.com/o/r.git#tok",
        ):
            with pytest.raises(ValueError, match="carries no credentials"):
                _parse_source({"git": {"url": url}, "path": "c.py"})
        # An "@" past the host is a path, not a credential.
        source = _parse_source({"git": {"url": "https://x/o/r@v2"}, "path": "c.py"})
        assert source.git_url == "https://x/o/r@v2"

    def test_a_check_name_that_no_row_could_select_is_refused(self):
        with pytest.raises(ValueError, match="not an identifier"):

            @check("price-matches")
            def _(ctx):
                return True

    def test_a_blocked_import_fails_a_distinct_later_source_with_the_lock_reason(
        self, tmp_path, monkeypatch
    ):
        """The lock cannot be taken back from a stuck import; the next source says so."""
        from hopsworks_agents.eval.checks import _preload

        good = tmp_path / "good.py"
        good.write_text(VERDICT_FILE.format(verdict="True"))
        other = tmp_path / "other.py"
        other.write_text(VERDICT_FILE.format(verdict="True"))
        blocked = _write(
            tmp_path,
            "blocked.py",
            """
            import time
            time.sleep(1.5)
            @check(needs_trace=False)
            def v(ctx):
                return True
            """,
        )
        monkeypatch.setattr(checks_module, "DEFAULT_TIMEOUT_S", 0.3)
        monkeypatch.setattr(checks_module, "IMPORT_LOCK_WAIT_S", 0.2)
        sources = _CheckSources(dataset_api=object())
        sources._download = lambda path: path  # type: ignore[method-assign]
        rows = _evaluators_from_spec(
            [
                {"type": "python", "name": "first", "file": str(good)},
                {"type": "python", "name": "stuck", "file": str(blocked)},
                {"type": "python", "name": "other", "file": str(other)},
            ],
            sources=sources,
        )
        started = time.monotonic()
        _preload(rows)
        results = _run_evaluators(rows, _task(), _trial(), None)
        assert time.monotonic() - started < 3
        by_row = {r.assertions.get("row"): r for r in results}
        assert by_row["first"].passed is True
        assert "importing the source took longer" in by_row["stuck"].reason
        assert "another source's import has not finished" in by_row["other"].reason
        # Once the stuck import ends, the lock is free and the source loads.
        time.sleep(1.6)
        [again] = _evaluators_from_spec(
            [{"type": "python", "name": "other", "file": str(other)}],
            sources=_CheckSources(dataset_api=object()),
        )
        again._sources._download = lambda path: path  # type: ignore[method-assign]
        assert _rows([again])["verdict"].passed is True


class TestPinningEdges:
    def _verdict_clone(self, verdict: str, *, files: int = 0):
        def clone(url, ref, commit, into):  # noqa: ARG001
            root = Path(into) / "repo"
            (root / "c").mkdir(parents=True)
            (root / "c" / "k.py").write_text(VERDICT_FILE.format(verdict=verdict))
            for i in range(files):
                (root / "c" / f"extra{i}.txt").write_text("x")
            (root / ".git").mkdir()
            (root / ".git" / "HEAD").write_text("abc123def\n")
            clone.calls.append((url, ref, commit))
            return str(root)

        clone.calls = []
        return clone

    ROW = {"type": "python", "name": "checks", "git": {"url": "https://x/r.git", "ref": "main"}, "path": "c/k.py"}  # fmt: skip
    SNAPSHOT_DIR = "/Projects/p/Resources/agent_evals/suites/s1/v1/checks"

    def _sources(self, api, tmp_path, clone):
        return _CheckSources(
            dataset_api=api,
            project_name="p",
            snapshot_dir=self.SNAPSHOT_DIR,
            workdir=str(tmp_path),
            clone=clone,
        )

    def test_the_loser_of_the_pin_race_grades_with_the_winners_copy(self, tmp_path):
        api = _FakeDatasetApi()
        a = self._sources(api, tmp_path / "a", self._verdict_clone("True"))
        b = self._sources(api, tmp_path / "b", self._verdict_clone("False"))
        # b looked for a snapshot before a pinned one: it sees none and clones.
        b._snapshot_of = lambda source: None  # type: ignore[method-assign]
        pins = []
        real_pin = b._pin
        b._pin = lambda *args, **kw: pins.append(1) or real_pin(*args, **kw)  # type: ignore[method-assign]

        result_a = _rows(_evaluators_from_spec([self.ROW], sources=a))["verdict"]
        result_b = _rows(_evaluators_from_spec([self.ROW], sources=b))["verdict"]
        assert pins == [1]
        # b's own clone said False; it graded with a's frozen copy.
        assert (result_a.passed, result_b.passed) == (True, True)
        assert result_b.assertions["source"]["origin"].startswith("snapshot ")
        assert len([f for f in api.files if f.endswith("/manifest.json")]) == 1
        assert not any(".staging-" in f for f in api.files)

    def test_a_failed_move_with_nothing_frozen_grades_with_the_live_checkout(
        self, tmp_path
    ):
        api = _FakeDatasetApi()

        def flaky_move(source, destination, overwrite=False):  # noqa: ARG001
            raise RuntimeError("namenode: connection reset")

        api.move = flaky_move  # type: ignore[method-assign]
        sources = self._sources(api, tmp_path, self._verdict_clone("True"))
        result = _rows(_evaluators_from_spec([self.ROW], sources=sources))["verdict"]
        assert result.passed is True
        assert result.assertions["source"]["pinned"] is False
        assert result.assertions["source"]["origin"].startswith("git ")
        assert not any(f.endswith("/manifest.json") for f in api.files)

    def test_a_repository_over_the_cap_is_pinned_by_commit(self, tmp_path, monkeypatch):
        monkeypatch.setattr(checks_module, "MAX_TREE_FILES", 2)
        api = _FakeDatasetApi()
        first = self._sources(api, tmp_path / "1", self._verdict_clone("True", files=5))
        result = _rows(_evaluators_from_spec([self.ROW], sources=first))["verdict"]
        assert result.passed is True
        source = result.assertions["source"]
        assert source["pinned"] is True and source["commit"] == "abc123def"
        [manifest_path] = [f for f in api.files if f.endswith("/manifest.json")]
        manifest = json.loads(api.files[manifest_path])
        assert manifest["tree"] is False and manifest["commit"] == "abc123def"
        assert not any("/tree/" in f for f in api.files)

        # The next run clones at the pinned commit, whatever the branch does.
        later_clone = self._verdict_clone("True")
        later = self._sources(api, tmp_path / "2", later_clone)
        result = _rows(_evaluators_from_spec([self.ROW], sources=later))["verdict"]
        assert later_clone.calls == [("https://x/r.git", "main", "abc123def")]
        assert result.assertions["source"]["origin"].endswith("(commit only)")
        assert result.assertions["source"]["pinned"] is True

    def test_a_workdir_is_made_on_first_use_and_removed_on_close(self):
        sources = _CheckSources()
        assert sources._made_workdir is None
        made = sources._workdir
        assert os.path.isdir(made)
        sources._close()
        assert not os.path.exists(made)
        assert sources._made_workdir is None


class TestWhatGradedIsWhatIsRecorded:
    """`assertions.source` names the code that graded: every file, as read, under the runner's key."""

    def test_a_packages_submodules_are_not_reused_by_the_next_import(self, tmp_path):
        pkg = tmp_path / "pkg"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("from . import pricing\n")

        def version(verdict):
            (pkg / "pricing.py").write_text(VERDICT_FILE.format(verdict=verdict))

        version("True")
        first = _rows([_file_evaluator(pkg, "p")])["verdict"]
        assert first.passed is True
        assert not any(
            n.startswith("hopsworks_check_") and "." in n for n in sys.modules
        ), "a package's submodule stayed in sys.modules"
        version("False")
        second = _rows([_file_evaluator(pkg, "p")])["verdict"]
        assert second.passed is False
        assert (
            first.assertions["source"]["sha256"]
            != second.assertions["source"]["sha256"]
        )

    def test_the_hash_covers_every_file_the_import_read(self, tmp_path):
        (tmp_path / "helpers.py").write_text("def verdict():\n    return True\n")
        (tmp_path / "checks.py").write_text(
            "from hopsworks_agents.eval.checks import check\n"
            "from helpers import verdict\n"
            "@check(needs_trace=False)\ndef v(ctx):\n    return verdict()\n"
        )
        before = _rows([_file_evaluator(tmp_path / "checks.py", "c")])["v"]
        assert before.passed is True
        assert sorted(before.assertions["source"]["files"]) == [
            "checks.py",
            "helpers.py",
        ]
        (tmp_path / "helpers.py").write_text("def verdict():\n    return False\n")
        after = _rows([_file_evaluator(tmp_path / "checks.py", "c")])["v"]
        assert after.passed is False
        assert (
            before.assertions["source"]["sha256"]
            != after.assertions["source"]["sha256"]
        )

    def test_the_hash_is_of_the_bytes_that_ran_not_a_second_read(self, tmp_path):
        """A stale bytecode cache must not run code the hash does not describe."""
        import py_compile

        path = tmp_path / "c.py"
        path.write_text(VERDICT_FILE.format(verdict="True"))
        # The cache says True; the source now says False, with the same mtime.
        py_compile.compile(
            str(path), cfile=str(tmp_path / "__pycache__" / "c.cpython-x.pyc")
        )
        stat = path.stat()
        path.write_text(VERDICT_FILE.format(verdict="False"))
        os.utime(path, (stat.st_atime, stat.st_mtime))
        row = _rows([_file_evaluator(path, "c")])["verdict"]
        assert row.passed is False
        assert (
            row.assertions["source"]["sha256"]
            == hashlib.sha256(b"c.py\0" + path.read_bytes()).hexdigest()
        )

    def test_a_checks_own_source_key_does_not_erase_the_provenance(self, tmp_path):
        path = _write(
            tmp_path,
            "own.py",
            """
            @check(needs_trace=False)
            def v(ctx):
                return CheckResult(passed=True, assertions={"source": "price catalogue", "row": 3})
            """,
        )
        row = _rows([_file_evaluator(path, "r")])["v"]
        assert row.assertions["source"]["sha256"]
        assert row.assertions["row"] == "r"
        assert row.assertions["check_reserved"] == {
            "source": "price catalogue",
            "row": 3,
        }

    def test_passed_must_be_a_bool(self):
        @check(needs_trace=False)
        def relays(ctx):
            return {"passed": "false"}

        @check(needs_trace=False)
        def relays_too(ctx):
            return CheckResult(passed="no")  # type: ignore[arg-type]

        for fn in (relays, relays_too):
            row = run_check(fn, answer="x")
            assert row.ungradable is True and "not a bool" in row.reason

    def test_odd_keys_cycles_and_non_finite_numbers_are_still_written(self):
        from decimal import Decimal

        loop: dict = {"n": 1}
        loop["self"] = loop

        @check(needs_trace=False)
        def odd(ctx):
            return CheckResult(
                passed=True,
                assertions={
                    (1, 2): "pair",
                    Decimal("1"): "one",
                    "nan": float("nan"),
                    "loop": loop,
                },
            )

        row = run_check(odd, answer="x")
        assert row.passed is True
        text = json.dumps(row.assertions, allow_nan=False)
        assert "<cycle>" in text and "nan" in text
        assert row.assertions["(1, 2)"] == "pair" and row.assertions["1"] == "one"

    def test_a_snapshot_of_exactly_the_cap_is_read_back(self, tmp_path, monkeypatch):
        monkeypatch.setattr(checks_module, "MAX_TREE_FILES", 2)

        def clone(url, ref, commit, into):  # noqa: ARG001
            root = Path(into) / "repo"
            (root / "c").mkdir(parents=True)
            (root / "c" / "k.py").write_text(VERDICT_FILE.format(verdict="True"))
            (root / "c" / "extra.txt").write_text("x")
            return str(root)

        api = _FakeDatasetApi()
        snapshot_dir = "/Projects/p/Resources/agent_evals/suites/s1/v1/checks"
        row = {"type": "python", "name": "checks", "git": {"url": "https://x/r.git", "ref": "main"}, "path": "c/k.py"}  # fmt: skip
        first = _CheckSources(dataset_api=api, project_name="p", snapshot_dir=snapshot_dir, workdir=str(tmp_path / "1"), clone=clone)  # fmt: skip
        assert (
            _rows(_evaluators_from_spec([row], sources=first))["verdict"].passed is True
        )
        [manifest] = [f for f in api.files if f.endswith("/manifest.json")]
        assert json.loads(api.files[manifest])["tree"] is True
        later = _CheckSources(dataset_api=api, project_name="p", snapshot_dir=snapshot_dir, workdir=str(tmp_path / "2"), clone=clone)  # fmt: skip
        result = _rows(_evaluators_from_spec([row], sources=later))["verdict"]
        assert result.passed is True
        assert result.assertions["source"]["origin"].startswith("snapshot ")


class TestRoundSix:
    """Namespace folders, a check's own `skipped`, and one hash formula."""

    @staticmethod
    def _source_with_lib(root: Path, verdict: str) -> Path:
        (root / "lib").mkdir(parents=True)
        (root / "lib" / "helpers.py").write_text(
            f"def verdict():\n    return {verdict}\n"
        )
        (root / "c.py").write_text(
            "from hopsworks_agents.eval.checks import check\n"
            "from lib.helpers import verdict\n"
            "@check(needs_trace=False)\ndef v(ctx):\n    return verdict()\n"
        )
        return root / "c.py"

    def test_a_helper_folder_without_init_does_not_leak_between_sources(self, tmp_path):
        a = self._source_with_lib(tmp_path / "a", "True")
        b = self._source_with_lib(tmp_path / "b", "False")
        first = _rows([_file_evaluator(a, "a")])["v"]
        assert first.passed is True
        assert "lib" not in sys.modules and "lib.helpers" not in sys.modules
        second = _rows([_file_evaluator(b, "b")])["v"]
        assert second.passed is False
        assert "lib/helpers.py" in second.assertions["source"]["files"]
        assert (
            first.assertions["source"]["sha256"]
            != second.assertions["source"]["sha256"]
        )

    def test_a_plain_folder_named_like_the_standard_library_does_not_shadow_it(
        self, tmp_path
    ):
        sys.modules.pop("statistics", None)
        (tmp_path / "statistics").mkdir()
        (tmp_path / "statistics" / "notes.txt").write_text("not a module")
        path = _write(
            tmp_path,
            "c.py",
            """
            import statistics

            @check(needs_trace=False)
            def v(ctx):
                return statistics.mean([1, 3]) == 2
            """,
        )
        assert _rows([_file_evaluator(path, "c")])["v"].passed is True
        assert getattr(sys.modules["statistics"], "__file__", None)

    def test_a_checks_own_skipped_does_not_make_the_trial_a_harness_error(
        self, tmp_path
    ):
        from hopsworks_agents.eval.evaluators import _was_skipped

        path = _write(
            tmp_path,
            "counts.py",
            """
            @check(needs_trace=False)
            def counts(ctx):
                return CheckResult(
                    passed=True,
                    assertions={"checked": 4, "skipped": 1, "source": "s", "check_source": "kept"},
                )
            """,
        )
        rows = _rows([_file_evaluator(path, "r")])
        row = rows["counts"]
        assert row.passed is True
        assert not _was_skipped([row])
        assert row.assertions["checked"] == 4
        assert row.assertions["check_source"] == "kept"
        assert row.assertions["check_reserved"] == {"skipped": 1, "source": "s"}

    def test_a_pinned_snapshots_manifest_and_its_results_agree(self, tmp_path):
        def clone(url, ref, commit, into):  # noqa: ARG001
            root = Path(into) / "repo"
            (root / "c").mkdir(parents=True)
            (root / "c" / "k.py").write_text(VERDICT_FILE.format(verdict="True"))
            return str(root)

        api = _FakeDatasetApi()
        snapshot_dir = "/Projects/p/Resources/agent_evals/suites/s1/v1/checks"
        row = {"type": "python", "name": "checks", "git": {"url": "https://x/r.git", "ref": "main"}, "path": "c/k.py"}  # fmt: skip
        sources = _CheckSources(dataset_api=api, project_name="p", snapshot_dir=snapshot_dir, workdir=str(tmp_path), clone=clone)  # fmt: skip
        result = _rows(_evaluators_from_spec([row], sources=sources))["verdict"]
        [manifest] = [f for f in api.files if f.endswith("/manifest.json")]
        assert (
            json.loads(api.files[manifest])["sha256"]
            == result.assertions["source"]["sha256"]
        )
