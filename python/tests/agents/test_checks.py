"""User-defined checks: declared with a decorator, resolved by the job, graded like any other.

The spec never carries code. A row names a file, a repository or a module and the
check it wants from it; the job resolves that into a function and shows it the one
stable shape, `CheckContext`. Everything a check can get wrong -- raising, hanging,
returning nonsense, not existing -- is the harness failing, never a pass.
"""

from __future__ import annotations

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
    PythonCheckEvaluator,
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

def plain(ctx):
    return True

def slow(ctx):
    import time
    time.sleep(5)
    return True

def explodes(ctx):
    raise RuntimeError("boom")
"""


@pytest.fixture
def check_file(tmp_path: Path) -> Path:
    path = tmp_path / "pricing.py"
    path.write_text(textwrap.dedent(CHECK_FILE))
    return path


# ── declaring ───────────────────────────────────────────────────────────────


class TestDecorator:
    def test_bare_named_and_configured_forms(self, check_file):
        module, _held = _import_path(str(check_file))
        declared = _declared_in(module)
        assert set(declared) == {"price_matches_catalogue", "answers_in_english"}
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
                {"type": "python", "name": "a", "file": "x.py", "function": "1bad"},
                "identifier",
            ),
            (
                {
                    "type": "python",
                    "name": "a",
                    "file": "x.py",
                    "check": "c",
                    "function": "f",
                },
                "not both",
            ),
            ({"type": "python", "name": "a", "file": "x.py", "params": []}, "object"),
            (
                {"type": "python", "name": "a", "file": "x.py", "timeout_s": 5000},
                "between 0 and 600",
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
        assert isinstance(evaluator, PythonCheckEvaluator)
        assert evaluator.name == "x"


# ── grading ─────────────────────────────────────────────────────────────────


def _file_evaluator(check_file: Path, name: str, **entry) -> PythonCheckEvaluator:
    sources = _CheckSources(dataset_api=object(), project_name="p")
    # The file is local: stand in for the download with the path itself.
    sources._download = lambda path: path  # type: ignore[method-assign]
    [evaluator] = _evaluators_from_spec(
        [{"type": "python", "name": name, "file": str(check_file), **entry}],
        sources=sources,
    )
    return evaluator


class TestGrading:
    def test_a_declared_check_is_found_by_name_and_sees_the_context(self, check_file):
        evaluator = _file_evaluator(check_file, "price_matches_catalogue")
        trace = _trace({"name": "lookup_product", "result": '{"price": 19.99}'})
        [result] = _run_evaluators([evaluator], _task(), _trial(), trace)
        assert result.passed is True
        assert result.evaluator_type == "python"
        assert result.assertions["quoted"] == 19.99
        assert result.assertions["source"]["origin"].startswith("hopsfs ")
        assert result.assertions["source"]["sha256"]

    def test_params_override_the_defaults(self, check_file):
        evaluator = _file_evaluator(
            check_file, "price_matches_catalogue", params={"tolerance": 5}
        )
        trace = _trace({"name": "lookup_product", "result": '{"price": 15.00}'})
        [result] = _run_evaluators([evaluator], _task(), _trial(), trace)
        assert result.passed is True

    def test_ungradable_when_the_precondition_did_not_hold(self, check_file):
        evaluator = _file_evaluator(check_file, "price_matches_catalogue")
        [result] = _run_evaluators([evaluator], _task(), _trial(), _trace())
        assert result.ungradable is True
        assert "never looked" in result.reason

    def test_no_trace_is_ungradable_for_a_check_that_needs_one(self, check_file):
        evaluator = _file_evaluator(check_file, "price_matches_catalogue")
        [result] = _run_evaluators([evaluator], _task(), _trial(), None)
        assert result.ungradable is True
        assert result.passed is None

    def test_a_check_declared_not_to_need_a_trace_grades_without_one(self, check_file):
        evaluator = _file_evaluator(check_file, "answers_in_english")
        [result] = _run_evaluators([evaluator], _task(), _trial("the price"), None)
        assert result.passed is True
        assert result.score == 1.0

    def test_spec_overrides_the_declaration(self, check_file):
        evaluator = _file_evaluator(check_file, "answers_in_english", needs_trace=True)
        [result] = _run_evaluators([evaluator], _task(), _trial("the price"), None)
        assert result.ungradable is True

    def test_a_plain_function_by_function_key(self, check_file):
        evaluator = _file_evaluator(check_file, "anything", function="plain")
        [result] = _run_evaluators([evaluator], _task(), _trial(), _trace())
        assert result.passed is True
        assert result.evaluator_name == "anything"

    def test_check_key_names_the_declared_check_under_another_row_name(
        self, check_file
    ):
        evaluator = _file_evaluator(
            check_file, "english_v2", check="answers_in_english"
        )
        [result] = _run_evaluators([evaluator], _task(), _trial("the x"), _trace())
        assert result.evaluator_name == "english_v2"
        assert result.passed is True

    def test_a_raising_check_is_the_harness_failing(self, check_file):
        evaluator = _file_evaluator(check_file, "x", function="explodes")
        [result] = _run_evaluators([evaluator], _task(), _trial(), _trace())
        assert result.ungradable is True
        assert "boom" in result.reason
        assert not result.assertions.get(SKIPPED)

    def test_a_hanging_check_times_out_without_a_verdict(self, check_file):
        evaluator = _file_evaluator(check_file, "x", function="slow", timeout_s=0.2)
        started = time.monotonic()
        [result] = _run_evaluators([evaluator], _task(), _trial(), _trace())
        assert time.monotonic() - started < 3
        assert result.ungradable is True
        assert "timed out" in result.reason

    def test_a_module_that_blocks_at_import_is_bounded_by_the_timeout(self, tmp_path):
        slow = tmp_path / "slow.py"
        slow.write_text(
            "import time\ntime.sleep(5)\n"
            "from hopsworks_agents.eval.checks import check\n"
            "@check(needs_trace=False)\ndef v(ctx):\n    return True\n"
        )
        evaluator = _file_evaluator(slow, "v", timeout_s=0.2)
        started = time.monotonic()
        [result] = _run_evaluators([evaluator], _task(), _trial(), None)
        assert time.monotonic() - started < 3
        assert result.assertions[SKIPPED] is True
        assert "importing the source took longer" in result.reason

    def test_a_check_calling_sys_exit_is_the_check_failing(self, tmp_path):
        leaving = tmp_path / "leaving.py"
        leaving.write_text(
            "import sys\nfrom hopsworks_agents.eval.checks import check\n"
            "@check(needs_trace=False)\ndef v(ctx):\n    sys.exit(3)\n"
        )
        evaluator = _file_evaluator(leaving, "v")
        [result] = _run_evaluators([evaluator], _task(), _trial(), None)
        assert result.ungradable is True
        assert "SystemExit" in result.reason

    def test_a_missing_check_is_skipped_on_every_trial(self, check_file):
        evaluator = _file_evaluator(check_file, "no_such_check")
        results = [
            _run_evaluators([evaluator], _task(), _trial(), _trace())[0]
            for _ in range(2)
        ]
        for result in results:
            assert result.ungradable is True
            assert result.assertions[SKIPPED] is True
            assert "no check named 'no_such_check'" in result.reason
            assert "price_matches_catalogue" in result.reason

    def test_an_unreadable_file_is_skipped_not_failed(self, tmp_path):
        evaluator = _file_evaluator(tmp_path / "missing.py", "x")
        [result] = _run_evaluators([evaluator], _task(), _trial(), _trace())
        assert result.assertions[SKIPPED] is True
        assert result.passed is None

    def test_no_sources_means_only_modules_load(self):
        [evaluator] = _evaluators_from_spec(
            [{"type": "python", "name": "x", "file": "Resources/a.py"}]
        )
        [result] = _run_evaluators([evaluator], _task(), _trial(), _trace())
        assert result.assertions[SKIPPED] is True
        assert "no way to read" in result.reason

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
            [{"type": "python", "name": "short", "module": "customer_checks"}]
        )
        [result] = _run_evaluators([evaluator], _task(), _trial("ok"), None)
        assert result.passed is True
        assert result.assertions["source"]["origin"].startswith(
            "module customer_checks"
        )

    def test_a_package_directory_loads_its_init(self, tmp_path):
        package = tmp_path / "checks"
        package.mkdir()
        (package / "helpers.py").write_text("def yes():\n    return True\n")
        (package / "__init__.py").write_text(
            "from hopsworks_agents.eval.checks import check\n"
            "from .helpers import yes\n"
            "@check(needs_trace=False)\ndef uses_helper(ctx):\n    return yes()\n"
        )
        evaluator = _file_evaluator(package, "uses_helper")
        [result] = _run_evaluators([evaluator], _task(), _trial(), None)
        assert result.passed is True


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
        module, _held = _import_path(str(check_file))
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
            transcript="user: meaning?\\nagent: 42",
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
        prefix = "/" + path.strip("/") + "/"
        children = set()
        for f in self.files:
            if f.startswith(prefix):
                rest = f[len(prefix) :]
                children.add(prefix + rest.split("/", 1)[0])
        return sorted(children)

    def download(self, path, local, overwrite=True):  # noqa: ARG002
        os.makedirs(os.path.dirname(local), exist_ok=True)
        Path(local).write_text(self.files["/" + path.lstrip("/")])
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
            [
                {
                    "type": "python",
                    "name": "answers_in_english",
                    "file": "Resources/agent_evals/checks/pricing.py",
                }
            ],
            sources=sources,
        )
        [result] = _run_evaluators([evaluator], _task(), _trial("the x"), None)
        assert result.passed is True
        assert result.assertions["source"]["origin"] == (
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
            [{"type": "python", "name": "uses_helper", "file": "Resources/checks"}],
            sources=sources,
        )
        [result] = _run_evaluators([evaluator], _task(), _trial(), None)
        assert result.passed is True

    def test_a_file_frozen_by_publish_is_read_from_the_snapshot(self, tmp_path):
        """The backend repoints a published row at its copy; the job reads that and says so."""
        snapshot_dir = "/Projects/p/Resources/agent_evals/suites/s1/v2/checks"
        code = (
            "from hopsworks_agents.eval.checks import check\n"
            "@check(needs_trace=False)\ndef which(ctx):\n    return True\n"
        )
        api = _FakeDatasetApi(
            {
                "/Projects/p/Resources/checks/c.py": code.replace("True", "False"),
                f"{snapshot_dir}/0123456789ab-c.py": code,
            }
        )
        sources = _CheckSources(
            dataset_api=api,
            project_name="p",
            snapshot_dir=snapshot_dir,
            workdir=str(tmp_path),
        )
        frozen, live = _evaluators_from_spec(
            [
                {
                    "type": "python",
                    "name": "which",
                    "file": f"{snapshot_dir}/0123456789ab-c.py",
                },
                {"type": "python", "name": "which", "file": "Resources/checks/c.py"},
            ],
            sources=sources,
        )
        [result] = _run_evaluators([frozen], _task(), _trial(), None)
        assert result.passed is True
        assert result.assertions["source"]["pinned"] is True
        assert result.assertions["source"]["origin"].startswith("snapshot ")
        [result] = _run_evaluators([live], _task(), _trial(), None)
        assert result.passed is False
        assert result.assertions["source"]["pinned"] is False

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

    def test_a_git_source_is_cloned_once_and_the_commit_recorded(self, tmp_path):
        clone = self._fake_clone(tmp_path)
        sources = _CheckSources(workdir=str(tmp_path), clone=clone)
        evaluators = _evaluators_from_spec(
            [
                {
                    "type": "python",
                    "name": "answers_in_english",
                    "git": {"url": "https://github.com/o/r.git", "ref": "main"},
                    "path": "evaluation/checks/pricing.py",
                },
                {
                    "type": "python",
                    "name": "price_matches_catalogue",
                    "git": {"url": "https://github.com/o/r.git", "ref": "main"},
                    "path": "evaluation/checks/pricing.py",
                },
            ],
            sources=sources,
        )
        trace = _trace({"name": "lookup_product", "result": '{"price": 19.99}'})
        results = _run_evaluators(evaluators, _task(), _trial("the 19.99"), trace)
        assert [r.passed for r in results] == [True, True]
        assert clone.calls == [("https://github.com/o/r.git", "main", "")]
        assert results[0].assertions["source"]["commit"] == "abc123def"
        assert results[0].assertions["source"]["pinned"] is False

    def _git_row(self, name="answers_in_english", url="https://github.com/o/r.git"):
        return {
            "type": "python",
            "name": name,
            "git": {"url": url, "ref": "main"},
            "path": "evaluation/checks/pricing.py",
        }

    def test_a_published_suite_pins_a_git_check_on_first_run(self, tmp_path):
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
        [evaluator] = _evaluators_from_spec(
            [self._git_row(url="https://user:tok@github.com/o/r.git")], sources=sources
        )
        [result] = _run_evaluators([evaluator], _task(), _trial("the x"), None)
        assert result.passed is True
        assert result.assertions["source"]["pinned"] is True

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
        assert "tok" not in json.dumps(manifest)
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
        [result] = _run_evaluators([again], _task(), _trial("the x"), None)
        assert result.assertions["source"]["pinned"] is True
        assert result.assertions["source"]["commit"] == "abc123def"
        assert result.assertions["source"]["origin"].startswith("snapshot ")
        assert later._clone.calls == []

    def test_a_pinned_check_keeps_the_helpers_it_imports_from_the_repository(
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
        row = {
            "type": "python",
            "name": "uses_shared",
            "git": {"url": "https://x/r.git", "ref": "main"},
            "path": "evaluation/checks/c.py",
        }
        first = _CheckSources(
            dataset_api=api,
            project_name="p",
            snapshot_dir=snapshot_dir,
            workdir=str(tmp_path / "a"),
            clone=clone,
        )
        [evaluator] = _evaluators_from_spec([row], sources=first)
        [result] = _run_evaluators([evaluator], _task(), _trial(), None)
        assert result.passed is True

        def no_clone(*args):  # noqa: ARG001
            raise AssertionError("the snapshot should have been read instead")

        later = _CheckSources(
            dataset_api=api,
            project_name="p",
            snapshot_dir=snapshot_dir,
            workdir=str(tmp_path / "b"),
            clone=no_clone,
        )
        [evaluator] = _evaluators_from_spec([row], sources=later)
        [result] = _run_evaluators([evaluator], _task(), _trial(), None)
        assert result.passed is True, result.reason
        assert result.assertions["source"]["pinned"] is True

    def test_two_first_runs_pin_once_and_the_loser_grades_with_the_winner(
        self, tmp_path
    ):
        """Two runs clone a branch that moved between them; one copy is frozen, both use it."""

        def clone_returning(verdict):
            def clone(url, ref, commit, into):  # noqa: ARG001
                root = Path(into) / "repo"
                (root / "c").mkdir(parents=True)
                (root / "c" / "k.py").write_text(
                    "from hopsworks_agents.eval.checks import check\n"
                    f"@check(needs_trace=False)\ndef v(ctx):\n    return {verdict}\n"
                )
                return str(root)

            return clone

        api = _FakeDatasetApi()
        snapshot_dir = "/Projects/p/Resources/agent_evals/suites/s1/v1/checks"
        row = {
            "type": "python",
            "name": "v",
            "git": {"url": "https://x/r.git", "ref": "main"},
            "path": "c/k.py",
        }
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
        [source_a] = [e.source for e in _evaluators_from_spec([row], sources=a)]
        fetched_a = a._fetch_local(source_a)
        fetched_b = b._fetch_local(source_a)
        assert fetched_a.pinned and fetched_b.pinned
        result_a = _run_evaluators(
            [_evaluators_from_spec([row], sources=a)[0]], _task(), _trial(), None
        )[0]
        result_b = _run_evaluators(
            [_evaluators_from_spec([row], sources=b)[0]], _task(), _trial(), None
        )[0]
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
            [
                {
                    "type": "python",
                    "name": "answers_in_english",
                    "git": "deployment",
                    "path": "evaluation/checks/pricing.py",
                }
            ],
            sources=sources,
        )
        [result] = _run_evaluators([evaluator], _task(), _trial("the x"), None)
        assert result.passed is True
        assert clone.calls == [("https://github.com/o/agent.git", "main", "fedcba9")]
        assert result.assertions["source"]["commit"] == "fedcba9"
        # Never pinned: the checks move with the agent.
        assert result.assertions["source"]["pinned"] is False
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
        [result] = _run_evaluators([evaluator], _task(), _trial(), None)
        assert result.assertions[SKIPPED] is True
        assert "not git-backed" in result.reason

    def test_a_failed_clone_names_no_credential(self, tmp_path):
        def clone(url, ref, commit, into):  # noqa: ARG001
            raise RuntimeError("fatal: could not read from https://u:secret@x/r")

        sources = _CheckSources(workdir=str(tmp_path), clone=clone)
        [evaluator] = _evaluators_from_spec(
            [
                {
                    "type": "python",
                    "name": "x",
                    "git": {"url": "https://u:secret@x/r", "ref": "main"},
                    "path": "c.py",
                }
            ],
            sources=sources,
        )
        [result] = _run_evaluators([evaluator], _task(), _trial(), None)
        assert result.assertions[SKIPPED] is True
        assert "secret" not in result.reason

    def test_helpers_of_the_same_name_do_not_leak_between_sources(self, tmp_path):
        """A second source's `helpers` must not be the first's, cached in sys.modules."""
        for name, verdict in (("a", "True"), ("b", "False")):
            folder = tmp_path / name
            folder.mkdir()
            (folder / "helpers.py").write_text(
                f"def verdict():\n    return {verdict}\n"
            )
            (folder / "checks.py").write_text(
                "from hopsworks_agents.eval.checks import check\n"
                "from helpers import verdict\n"
                "@check(needs_trace=False)\ndef v(ctx):\n    return verdict()\n"
            )
        sources = _CheckSources(dataset_api=object(), project_name="p")
        sources._download = lambda path: path  # type: ignore[method-assign]
        a, b = _evaluators_from_spec(
            [
                {
                    "type": "python",
                    "name": "v",
                    "file": str(tmp_path / "a" / "checks.py"),
                },
                {
                    "type": "python",
                    "name": "v",
                    "file": str(tmp_path / "b" / "checks.py"),
                },
            ],
            sources=sources,
        )
        results = _run_evaluators([a, b], _task(), _trial(), None)
        assert [r.passed for r in results] == [True, False]
        assert "helpers" not in sys.modules
        assert str(tmp_path / "a") not in sys.path

    def test_two_files_of_the_same_name_do_not_collide(self, tmp_path):
        first = tmp_path / "a" / "checks.py"
        second = tmp_path / "b" / "checks.py"
        first.parent.mkdir()
        second.parent.mkdir()
        first.write_text(
            "from hopsworks_agents.eval.checks import check\n"
            "@check(needs_trace=False)\ndef verdict(ctx):\n    return True\n"
        )
        second.write_text(
            "from hopsworks_agents.eval.checks import check\n"
            "@check(needs_trace=False)\ndef verdict(ctx):\n    return False\n"
        )
        sources = _CheckSources(dataset_api=object(), project_name="p")
        sources._download = lambda path: path  # type: ignore[method-assign]
        a, b = _evaluators_from_spec(
            [
                {"type": "python", "name": "verdict", "file": str(first)},
                {"type": "python", "name": "verdict", "file": str(second)},
            ],
            sources=sources,
        )
        results = _run_evaluators([a, b], _task(), _trial(), None)
        assert [r.passed for r in results] == [True, False]


class TestRunJobWiring:
    def test_check_sources_pins_only_a_published_suite(self, monkeypatch):
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
        assert (
            "python"
            in __import__(
                "hopsworks_agents.eval.evaluator_spec", fromlist=["SPEC_TYPES"]
            ).SPEC_TYPES
        )
