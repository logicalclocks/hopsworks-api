"""A judge configured with named criteria, weights and floors.

The two behaviours worth protecting: a weighted total cannot rescue a
catastrophic score on a criterion someone marked critical, and a judge that
answers only half the criteria is ungradable rather than averaged over whatever
came back.
"""

from __future__ import annotations

import json

import pytest
from hopsworks_agents.eval import judge_config as jc
from hopsworks_agents.eval.evaluator_spec import SpecError, evaluators_from_spec
from hopsworks_agents.eval.judge_config import (
    FAILURE_CATEGORIES,
    PROVIDERS,
    REASONING_EFFORTS,
    JudgeConfigError,
    default_templates,
    parse_judge_config,
    render_prompt,
)
from hopsworks_agents.eval.judges import LlmJudgeEvaluator
from hopsworks_agents.eval.models import Task, Trial


CRITERIA = {
    "task_completion": {"weight": 0.5, "description": "Did it finish the job?"},
    "correctness": {"weight": 0.3, "description": "Is it factually right?"},
    "safety": {"weight": 0.2, "description": "Did it respect the limits?"},
}


def entry(**overrides):
    base = {
        "type": "llm_judge",
        "score_range": [1, 5],
        "criteria": CRITERIA,
        "thresholds": {"pass_score": 4.0, "critical_dimensions": {"safety": 4}},
    }
    base.update(overrides)
    return base


def _expects(kwargs: dict) -> dict:
    """Legacy per-field kwargs, written as the expectations they now are.

    Expectations are keyed by the check that reads them, and a check's name
    defaults to its type — so `required_tools=["x"]` is what the `tool_call` and
    `tool_order` checks expect, and an expected answer is what every check that
    reads one expects. Written once here so a test can still say the thing it
    means.
    """
    import json as _json

    expectations = dict(kwargs.pop("expectations", {}) or {})
    answer = kwargs.pop("expected_output", None)
    if answer is not None:
        for name in ("exact_match", "contains", "pairwise", "llm_judge"):
            expectations.setdefault(name, answer)
    rubric = kwargs.pop("rubric", None)
    if rubric is not None:
        expectations["llm_judge"] = rubric
    required = kwargs.pop("required_tools", None)
    forbidden = kwargs.pop("forbidden_tools", None)
    if required is not None or forbidden is not None:
        expectations["tool_call"] = _json.dumps(
            {"required": required or [], "forbidden": forbidden or []}
        )
        expectations["tool_order"] = ", ".join(required or [])
        expectations["no_unnecessary_tools"] = ", ".join(required or [])
    if expectations:
        kwargs["expectations"] = expectations
    return kwargs


def task(**kwargs) -> Task:
    kwargs = _expects(kwargs)
    return Task(
        task_id="t1",
        input_messages=json.dumps([{"role": "user", "content": "cancel 4471"}]),
        **kwargs,
    )


def trial(output: str = "Done — order 4471 is cancelled.") -> Trial:
    return Trial(
        trial_id="x",
        run_id="r",
        task_id="t1",
        task_version=1,
        trial_index=0,
        deployment_id=1,
        final_output=output,
    )


def judge(reply: dict | str, **overrides) -> LlmJudgeEvaluator:
    text = reply if isinstance(reply, str) else json.dumps(reply)
    return LlmJudgeEvaluator(lambda _p: text, parse_judge_config(entry(**overrides)))


class TestParsing:
    def test_criteria_as_an_object_of_settings(self):
        config = parse_judge_config(entry())
        assert [c.name for c in config.criteria] == [
            "task_completion",
            "correctness",
            "safety",
        ]
        assert config.criteria[2].critical_min == 4

    def test_criteria_as_a_plain_list_of_names(self):
        config = parse_judge_config(
            {"type": "llm_judge", "criteria": ["accuracy", "tone"]}
        )
        assert [c.weight for c in config.criteria] == [1.0, 1.0]

    def test_pass_score_defaults_to_three_quarters_of_the_scale(self):
        config = parse_judge_config(
            {"type": "llm_judge", "score_range": [1, 5], "criteria": ["a"]}
        )
        assert config.pass_score == 4.0

    def test_a_critical_floor_naming_no_criterion_is_refused(self):
        # silently ignoring it would leave someone believing a floor is enforced
        with pytest.raises(JudgeConfigError, match="no such criterion"):
            parse_judge_config(
                entry(
                    thresholds={"critical_dimensions": {"tone": 3}},
                )
            )

    def test_a_pass_score_outside_the_range_is_refused(self):
        with pytest.raises(JudgeConfigError, match="inside score_range"):
            parse_judge_config(entry(thresholds={"pass_score": 9}))

    def test_an_unknown_provider_is_refused(self):
        with pytest.raises(JudgeConfigError, match="provider must be"):
            parse_judge_config(entry(provider="mystery"))

    def test_an_unknown_input_is_refused(self):
        with pytest.raises(JudgeConfigError, match="unknown input"):
            parse_judge_config(entry(inputs=["user_request", "the_weather"]))

    def test_zero_weights_are_refused(self):
        with pytest.raises(JudgeConfigError, match="weights cannot all be zero"):
            parse_judge_config(entry(criteria={"a": {"weight": 0}}))

    def test_a_custom_template_must_have_the_answer_in_it(self):
        with pytest.raises(JudgeConfigError, match="prompt_template"):
            parse_judge_config(entry(prompt_template="grade {question} please"))

    def test_temperature_is_bounded(self):
        with pytest.raises(JudgeConfigError, match="temperature"):
            parse_judge_config(entry(temperature=5))

    def test_reasoning_effort_is_enumerated(self):
        assert (
            parse_judge_config(entry(reasoning_effort="High")).reasoning_effort
            == "high"
        )
        assert (
            parse_judge_config(entry(reasoningEffort="max")).reasoning_effort == "max"
        )
        with pytest.raises(JudgeConfigError, match="reasoning_effort"):
            parse_judge_config(entry(reasoning_effort="heroic"))

    def test_normalisation_maps_the_scale_onto_zero_to_one(self):
        config = parse_judge_config(entry())
        assert config.normalise(1) == 0.0
        assert config.normalise(5) == 1.0
        assert config.normalise(3) == 0.5

    def test_normalisation_clamps_a_judge_that_left_the_scale(self):
        # models told to answer 1-5 occasionally answer 0 or 6
        config = parse_judge_config(entry())
        assert config.normalise(0) == 0.0
        assert config.normalise(9) == 1.0


class TestPrompt:
    def test_it_names_every_criterion_with_its_description(self):
        prompt = render_prompt(parse_judge_config(entry()), question="q", answer="a")
        assert "task_completion: Did it finish the job?" in prompt
        assert "safety: Did it respect the limits?" in prompt

    def test_it_asks_for_the_configured_scale(self):
        prompt = render_prompt(parse_judge_config(entry()), question="q", answer="a")
        assert "1 to 5" in prompt

    def test_inputs_control_what_the_judge_is_shown(self):
        # a judge asked whether the agent could get there alone must not be
        # shown the answer
        config = parse_judge_config(entry(inputs=["user_request", "agent_response"]))
        prompt = render_prompt(config, question="q", answer="a", expected="the answer")
        assert "the answer" not in prompt

        seeing = parse_judge_config(entry(inputs=["expected_result"]))
        assert "the answer" in render_prompt(
            seeing, question="q", answer="a", expected="the answer"
        )

    def test_tool_context_appears_only_when_asked_for(self):
        config = parse_judge_config(
            entry(inputs=["user_request", "agent_response", "tool_results"])
        )
        prompt = render_prompt(
            config,
            question="q",
            answer="a",
            tool_calls="lookup()",
            tool_results="status=open",
        )
        assert "status=open" in prompt
        assert "lookup()" not in prompt

    def test_a_custom_template_can_ask_for_the_output_shape(self):
        prompt = render_prompt(
            parse_judge_config(
                entry(prompt_template="{question}|{answer}|reply with {output_shape}")
            ),
            question="q",
            answer="a",
        )

        # Without this a custom prompt cannot say what to return, and the reply
        # is unparseable -- reported as ungradable, which reads like a broken
        # judge rather than a prompt missing one line.
        assert '"scores"' in prompt
        assert "task_completion" in prompt

    def test_a_custom_template_gets_the_criteria_and_a_whole_number_range(self):
        prompt = render_prompt(
            parse_judge_config(
                entry(
                    prompt_template="{question}|{answer}|{criteria}|{score_min}-{score_max}"
                )
            ),
            question="q",
            answer="a",
        )

        assert "task_completion" in prompt
        # 1-5, not 1.0-5.0: the default path formats these and a custom one
        # showing the model a different scale would be the same config read two
        # ways.
        assert "1-5" in prompt

    def test_a_custom_template_replaces_the_built_in_one(self):
        prompt = render_prompt(
            parse_judge_config(entry(prompt_template="only this: {question} {answer}")),
            question="q",
            answer="a",
        )

        assert prompt == "only this: q a"
        assert "You are grading" not in prompt

    def test_the_failure_taxonomy_is_enumerated_in_the_prompt(self):
        prompt = render_prompt(parse_judge_config(entry()), question="q", answer="a")
        for category in FAILURE_CATEGORIES:
            assert category in prompt


class TestGrading:
    def test_a_weighted_total_above_the_bar_passes(self):
        result = judge(
            {"scores": {"task_completion": 5, "correctness": 4, "safety": 5}}
        ).grade(task(), trial(), None)
        assert result.passed
        assert result.assertions["weighted_score"] == pytest.approx(4.7)
        assert result.score == pytest.approx((4.7 - 1) / 4)

    def test_a_weighted_total_below_the_bar_fails_and_says_so(self):
        result = judge(
            {"scores": {"task_completion": 3, "correctness": 3, "safety": 4}}
        ).grade(task(), trial(), None)
        assert result.passed is False
        assert "needs 4" in result.reason

    def test_a_critical_floor_overrides_a_good_total(self):
        # the whole point: six good scores must not hide one catastrophic result
        result = judge(
            {"scores": {"task_completion": 5, "correctness": 5, "safety": 1}}
        ).grade(task(), trial(), None)
        assert result.passed is False
        assert result.assertions["critical_breached"] == ["safety"]
        assert "below the floor on safety" in result.reason

    def test_the_breakdown_is_kept_in_both_scales(self):
        result = judge(
            {"scores": {"task_completion": 5, "correctness": 4, "safety": 5}}
        ).grade(task(), trial(), None)
        assert result.assertions["criteria"]["correctness"] == 4
        assert result.assertions["criteria_normalised"]["correctness"] == 0.75

    def test_a_judge_scoring_only_some_criteria_is_ungradable(self):
        # a weighted total over whichever criteria came back is a different
        # measurement every time
        result = judge({"scores": {"task_completion": 5}}).grade(task(), trial(), None)
        assert result.ungradable
        assert "did not score correctness, safety" in result.reason

    def test_prose_instead_of_json_is_ungradable_not_a_failure(self):
        result = judge("Looks good to me!").grade(task(), trial(), None)
        assert result.ungradable and result.passed is False

    def test_a_judge_that_raises_is_ungradable(self):
        evaluator = LlmJudgeEvaluator(
            lambda _p: (_ for _ in ()).throw(RuntimeError("rate limited")),
            parse_judge_config(entry()),
        )
        result = evaluator.grade(task(), trial(), None)
        assert result.ungradable
        assert "rate limited" in result.reason

    def test_no_answer_is_ungradable(self):
        result = judge({"scores": {}}).grade(task(), trial(""), None)
        assert result.ungradable

    def test_an_unknown_failure_category_becomes_other(self):
        result = judge(
            {
                "scores": {"task_completion": 5, "correctness": 4, "safety": 5},
                "failure_category": "vibes",
            }
        ).grade(task(), trial(), None)
        assert result.assertions["failure_category"] == "other"

    def test_reasoning_reaches_the_reason_when_it_failed(self):
        result = judge(
            {
                "scores": {"task_completion": 2, "correctness": 2, "safety": 5},
                "reasoning": {"correctness": "the order id was invented"},
            }
        ).grade(task(), trial(), None)
        assert "the order id was invented" in result.reason

    def test_a_judge_reading_tool_calls_needs_a_trace(self):
        evaluator = judge(
            {"scores": {}}, inputs=["user_request", "agent_response", "tool_calls"]
        )
        assert evaluator.needs_trace is True
        assert evaluator.grade(task(), trial(), None).ungradable

    def test_a_judge_not_reading_tool_calls_does_not_need_one(self):
        # otherwise it would go ungradable for want of a trace it never reads
        evaluator = judge(
            {"scores": {"task_completion": 5, "correctness": 5, "safety": 5}}
        )
        assert evaluator.needs_trace is False
        assert evaluator.grade(task(), trial(), None).passed


class TestSpecIntegration:
    def test_criteria_are_carried_onto_the_evaluator(self):
        evaluators = evaluators_from_spec([entry()], judge_completer=lambda _p: "{}")
        assert [c.name for c in evaluators[0].config.criteria] == [
            "task_completion",
            "correctness",
            "safety",
        ]

    def test_a_judge_with_no_criteria_gets_one_called_overall(self):
        # not a different class, not a different code path — the same judge
        # scoring a single unnamed thing
        evaluators = evaluators_from_spec(
            [{"type": "llm_judge"}], judge_completer=lambda _p: "{}"
        )
        assert [c.name for c in evaluators[0].config.effective_criteria()] == [
            "overall"
        ]

    def test_every_judged_type_can_bring_its_own_model(self):
        # a pairwise comparison has no reason to be stuck with the project
        # default when a rubric judge is not
        import os

        os.environ["OPENAI_API_KEY"] = "for-this-test"
        try:
            for kind in ("pairwise", "tool_arguments_judge", "tool_result_used"):
                evaluators = evaluators_from_spec(
                    [{"type": kind, "provider": "openai", "model": "gpt-4o"}]
                )
                assert evaluators[0].model == "gpt-4o", kind
        finally:
            del os.environ["OPENAI_API_KEY"]

    def test_a_bad_judge_config_is_a_spec_error(self):
        # so it is refused at authoring time like every other malformed entry
        with pytest.raises(SpecError, match="provider must be"):
            evaluators_from_spec(
                [entry(provider="mystery")], judge_completer=lambda _p: ""
            )

    def test_a_judge_whose_variable_is_unset_is_skipped_not_failed(self, monkeypatch):
        # a project without a key still runs its deterministic checks, and the
        # trial reports what was skipped rather than pretending it was judged
        monkeypatch.delenv("NOPE", raising=False)
        evaluators = evaluators_from_spec(
            [entry(provider="openai", model="gpt-4o", api_key_env="NOPE")],
            judge_completer=lambda _p: "{}",
        )
        assert evaluators == []

    def test_a_judge_naming_a_set_variable_gets_its_own_provider(self, monkeypatch):
        # a suite can mix a cheap judge for canaries with an expensive one for a
        # release gate, each on its own key
        monkeypatch.setenv("MY_KEY", "its-own")
        evaluators = evaluators_from_spec(
            [entry(provider="openai", model="gpt-4o", api_key_env="MY_KEY")]
        )
        assert len(evaluators) == 1
        assert evaluators[0].config.provider == "openai"


def test_the_default_templates_all_build():
    for template in default_templates():
        evaluators = evaluators_from_spec(
            template["spec"], judge_completer=lambda _p: "{}"
        )
        assert evaluators, template["name"]


def test_the_default_template_leaves_tool_checks_to_the_tool_evaluators():
    # paying a model to decide whether a required tool ran is slower, costlier
    # and less reliable than the evaluator that knows
    spec = json.loads(default_templates()[0]["spec"])
    names = set(spec[0]["criteria"])
    assert not names & {"tool_selection", "tool_execution", "efficiency"}


class TestProviders:
    def test_every_provider_maps_to_an_adapter_that_exists(self):
        # the point of the registry: adding a provider is a row, not a client
        for key, entry in PROVIDERS.items():
            assert entry["adapter"] in ("openai", "anthropic"), key

    def test_only_anthropic_needs_its_own_client(self):
        anthropic = [k for k, v in PROVIDERS.items() if v["adapter"] == "anthropic"]
        assert anthropic == ["anthropic"]

    def test_every_openai_shaped_provider_knows_where_to_call(self):
        # except openai itself, whose SDK default is correct, and custom, which
        # is refused without one
        for key, entry in PROVIDERS.items():
            if entry["adapter"] == "openai" and key not in ("openai", "custom"):
                assert entry["base_url"].startswith("https://"), key

    def test_a_custom_provider_needs_a_base_url(self):
        with pytest.raises(JudgeConfigError, match="needs a base_url"):
            parse_judge_config({"type": "llm_judge", "provider": "custom"})

    def test_a_custom_provider_with_a_base_url_is_fine(self):
        config = parse_judge_config(
            {
                "type": "llm_judge",
                "provider": "custom",
                "base_url": "https://my-vllm.internal/v1",
            }
        )
        assert config.base_url == "https://my-vllm.internal/v1"

    def test_every_provider_parses(self):
        for key in PROVIDERS:
            entry = {"type": "llm_judge", "provider": key}
            if key == "custom":
                entry["base_url"] = "https://x/v1"
            assert parse_judge_config(entry).provider == key


class TestWhereAJudgesKeyComesFrom:
    """Every provider's own SDK reads a conventional variable, so a key already.

    set for anything else is found without being named again.
    """

    def config(self, provider="anthropic", named=""):
        from hopsworks_agents.eval.judge_config import JudgeConfig

        return JudgeConfig(provider=provider, api_key_env=named)

    def test_every_provider_but_custom_names_its_variable(self):
        # the whole point: without one there is nothing to look for
        from hopsworks_agents.eval.judge_config import PROVIDERS

        missing = [
            name
            for name, spec in PROVIDERS.items()
            if name != "custom" and not spec.get("env_var")
        ]
        assert missing == []

    def test_a_judge_naming_its_own_variable_uses_that_one(self, monkeypatch):
        # for a judge that needs a different key from everything else — a release
        # gate on its own quota
        from hopsworks_agents.eval.judge_config import api_key_for

        monkeypatch.setenv("ANTHROPIC_API_KEY", "ambient")
        monkeypatch.setenv("GATE_KEY", "its-own")
        assert api_key_for(self.config(named="GATE_KEY")) == "its-own"

    def test_the_providers_own_variable_is_the_default(self, monkeypatch):
        # what a project already has set, because every SDK reads it
        from hopsworks_agents.eval.judge_config import api_key_for

        monkeypatch.setenv("OPENAI_API_KEY", "from-env")
        assert api_key_for(self.config(provider="openai")) == "from-env"

        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        assert api_key_for(self.config()) is None

    def test_a_named_variable_that_is_unset_is_not_silently_replaced(self, monkeypatch):
        # naming one is a choice; falling back to the ambient key would grade with
        # a key the suite did not ask for
        from hopsworks_agents.eval.judge_config import api_key_for

        monkeypatch.setenv("ANTHROPIC_API_KEY", "ambient")
        monkeypatch.delenv("GATE_KEY", raising=False)
        assert api_key_for(self.config(named="GATE_KEY")) is None

    def test_a_custom_provider_has_nowhere_to_look(self, monkeypatch):
        # a gateway has no conventional variable, so it has to be told
        from hopsworks_agents.eval.judge_config import api_key_for

        assert api_key_for(self.config(provider="custom")) is None

    def test_the_variable_read_is_reportable(self):
        # so a skipped judge says what to set, rather than that something is absent
        from hopsworks_agents.eval.judge_config import api_key_source

        assert "GATE_KEY" in api_key_source(self.config(named="GATE_KEY"))
        assert "account" in api_key_source(self.config())


class TestWhenAModelRejectsTemperature:
    """Newer models fix their own sampling and refuse the parameter. Every judge.

    call failed with a 400, leaving every task graded by them ungradable, while
    the run reported SUCCEEDED.
    """

    def calls(self):
        seen = []

        def call(**extra):
            seen.append(extra)
            if "temperature" in extra:
                raise RuntimeError(
                    "Error code: 400 - `temperature` is deprecated for this model."
                )
            return "graded"

        return call, seen

    def test_it_retries_without_the_parameter(self):
        from hopsworks_agents.eval.judge_config import _without_rejected_temperature

        call, seen = self.calls()
        assert _without_rejected_temperature(call, 0.0) == "graded"
        assert seen == [{"temperature": 0.0}, {}]

    def test_a_model_that_accepts_it_is_called_once(self):
        # the retry is for the refusal, not a second call on every judgement
        from hopsworks_agents.eval.judge_config import _without_rejected_temperature

        seen = []

        def call(**extra):
            seen.append(extra)
            return "graded"

        assert _without_rejected_temperature(call, 0.7) == "graded"
        assert seen == [{"temperature": 0.7}]

    def test_any_other_400_is_raised(self):
        # a blind retry would swallow a real problem — a bad model name, no quota
        import pytest
        from hopsworks_agents.eval.judge_config import _without_rejected_temperature

        def call(**extra):
            raise RuntimeError("Error code: 400 - model not found")

        with pytest.raises(RuntimeError, match="model not found"):
            _without_rejected_temperature(call, 0.0)


class TestProviderRequestShape:
    @pytest.mark.parametrize(
        ("provider", "model"),
        [
            ("anthropic", "claude-sonnet-5"),
            ("anthropic", "claude-opus-5"),
            ("anthropic", "claude-opus-4-8"),
            ("anthropic", "claude-mythos-preview"),
            ("openai", "gpt-5.6-terra"),
            ("custom", "openrouter/openai/o3-mini"),
            ("google", "gemini-3.6-flash"),
            ("google", "gemini-3.5-flash-lite"),
            ("deepseek", "deepseek-v4-pro"),
            ("fireworks", "accounts/acme/models/kimi-k2.6"),
        ],
    )
    def test_fixed_sampling_models_do_not_send_temperature_first(self, provider, model):
        from hopsworks_agents.eval.judge_config import _TemperatureParameter

        seen = []

        def call(**extra):
            seen.append(extra)
            return "graded"

        temperature = _TemperatureParameter.for_model(provider, model, 0.0)
        assert temperature.call(call) == "graded"
        assert seen == [{}]

    @pytest.mark.parametrize(
        ("provider", "model"),
        [
            ("anthropic", "claude-sonnet-4-5"),
            ("openai", "gpt-4o"),
            ("google", "gemini-2.5-pro"),
            ("mistral", "mistral-medium-latest"),
            ("xai", "grok-4.5"),
        ],
    )
    def test_models_with_sampling_controls_still_send_temperature(
        self, provider, model
    ):
        from hopsworks_agents.eval.judge_config import _TemperatureParameter

        seen = []

        def call(**extra):
            seen.append(extra)
            return "graded"

        temperature = _TemperatureParameter.for_model(provider, model, 0.7)
        assert temperature.call(call) == "graded"
        assert seen == [{"temperature": 0.7}]

    @pytest.mark.parametrize(
        "model",
        ["gpt-5.6-terra", "openrouter/openai/o4-mini", "o3-mini"],
    )
    def test_openai_reasoning_chat_uses_completion_token_limit(self, model):
        from hopsworks_agents.eval.judge_config import _ChatCompletionParameters

        seen = []

        def call(**extra):
            seen.append(extra)
            return "graded"

        parameters = _ChatCompletionParameters("openai", model, 0.0, 1500)
        assert parameters.call(call) == "graded"
        assert seen == [{"max_completion_tokens": 1500}]

    def test_non_reasoning_chat_keeps_max_tokens_and_temperature(self):
        from hopsworks_agents.eval.judge_config import _ChatCompletionParameters

        seen = []

        def call(**extra):
            seen.append(extra)
            return "graded"

        parameters = _ChatCompletionParameters("openai", "gpt-4o", 0.2, 1500)
        assert parameters.call(call) == "graded"
        assert seen == [{"max_tokens": 1500, "temperature": 0.2}]

    def test_anthropic_reasoning_effort_uses_output_config(self):
        from hopsworks_agents.eval.judge_config import _AnthropicMessagesParameters

        seen = []

        def call(**extra):
            seen.append(extra)
            return "graded"

        parameters = _AnthropicMessagesParameters("claude-sonnet-5", 0.0, "high")
        assert parameters.call(call) == "graded"
        assert seen == [{"output_config": {"effort": "high"}}]

    def test_anthropic_omits_effort_for_models_that_do_not_support_it(self):
        from hopsworks_agents.eval.judge_config import _AnthropicMessagesParameters

        seen = []

        def call(**extra):
            seen.append(extra)
            return "graded"

        parameters = _AnthropicMessagesParameters("claude-sonnet-4-5", 0.0, "high")
        assert parameters.call(call) == "graded"
        assert seen == [{"temperature": 0.0}]

    def test_openai_reasoning_effort_is_top_level(self):
        from hopsworks_agents.eval.judge_config import _ChatCompletionParameters

        seen = []

        def call(**extra):
            seen.append(extra)
            return "graded"

        parameters = _ChatCompletionParameters(
            "openai", "gpt-5.6-terra", 0.0, 1500, "high"
        )
        assert parameters.call(call) == "graded"
        assert seen == [{"max_completion_tokens": 1500, "reasoning_effort": "high"}]

    def test_non_reasoning_chat_omits_reasoning_effort(self):
        from hopsworks_agents.eval.judge_config import _ChatCompletionParameters

        seen = []

        def call(**extra):
            seen.append(extra)
            return "graded"

        parameters = _ChatCompletionParameters("openai", "gpt-4o", 0.2, 1500, "high")
        assert parameters.call(call) == "graded"
        assert seen == [{"max_tokens": 1500, "temperature": 0.2}]

    def test_deepseek_reasoning_effort_enables_thinking(self):
        from hopsworks_agents.eval.judge_config import _ChatCompletionParameters

        seen = []

        def call(**extra):
            seen.append(extra)
            return "graded"

        parameters = _ChatCompletionParameters(
            "deepseek", "deepseek-v4-pro", 0.0, 1500, "max"
        )
        assert parameters.call(call) == "graded"
        assert seen == [
            {
                "max_tokens": 1500,
                "reasoning_effort": "max",
                "extra_body": {"thinking": {"type": "enabled"}},
            }
        ]

    def test_deepseek_none_disables_thinking(self):
        from hopsworks_agents.eval.judge_config import _ChatCompletionParameters

        seen = []

        def call(**extra):
            seen.append(extra)
            return "graded"

        parameters = _ChatCompletionParameters(
            "deepseek", "deepseek-v4-pro", 0.0, 1500, "none"
        )
        assert parameters.call(call) == "graded"
        assert seen == [
            {
                "max_tokens": 1500,
                "extra_body": {"thinking": {"type": "disabled"}},
            }
        ]

    def test_reasoning_effort_rejections_are_learned_for_later_calls(self):
        from hopsworks_agents.eval.judge_config import _ChatCompletionParameters

        seen = []

        def call(**extra):
            seen.append(extra)
            if "reasoning_effort" in extra:
                raise RuntimeError(
                    "Error code: 400 - unsupported parameter reasoning_effort"
                )
            return "graded"

        parameters = _ChatCompletionParameters(
            "custom", "gpt-oss-120b", 0.0, 1500, "high"
        )
        assert parameters.call(call) == "graded"
        assert parameters.call(call) == "graded"
        assert seen == [
            {"max_tokens": 1500, "temperature": 0.0, "reasoning_effort": "high"},
            {"max_tokens": 1500, "temperature": 0.0},
            {"max_tokens": 1500, "temperature": 0.0},
        ]

    def test_structured_chat_content_reads_text_blocks_only(self):
        from hopsworks_agents.eval.judge_config import _message_text

        assert (
            _message_text(
                [
                    {"type": "reasoning", "text": "private"},
                    {"type": "text", "text": '{"scores": {}}'},
                ]
            )
            == '{"scores": {}}'
        )

    def test_unknown_model_rejections_are_learned_for_later_calls(self):
        from hopsworks_agents.eval.judge_config import _ChatCompletionParameters

        seen = []

        def call(**extra):
            seen.append(extra)
            if "temperature" in extra:
                raise RuntimeError(
                    "Error code: 400 - `temperature` is deprecated for this model."
                )
            return "graded"

        parameters = _ChatCompletionParameters("custom", "new-fixed-model", 0.0, 1500)
        assert parameters.call(call) == "graded"
        assert parameters.call(call) == "graded"
        assert seen == [
            {"max_tokens": 1500, "temperature": 0.0},
            {"max_tokens": 1500},
            {"max_tokens": 1500},
        ]

    def test_unknown_gpt_style_token_limit_rejection_is_retried(self):
        from hopsworks_agents.eval.judge_config import _ChatCompletionParameters

        seen = []

        def call(**extra):
            seen.append(extra)
            if "max_tokens" in extra:
                raise RuntimeError(
                    "Unsupported parameter: 'max_tokens' is not supported with "
                    "this model. Use 'max_completion_tokens' instead."
                )
            return "graded"

        parameters = _ChatCompletionParameters("custom", "maybe-reasoning", 0.0, 1500)
        assert parameters.call(call) == "graded"
        assert seen == [
            {"max_tokens": 1500, "temperature": 0.0},
            {"max_completion_tokens": 1500, "temperature": 0.0},
        ]

    def test_reasoning_effort_values_stay_in_sync_with_the_parser(self):
        assert set(REASONING_EFFORTS) == {
            "none",
            "default",
            "minimal",
            "low",
            "medium",
            "high",
            "xhigh",
            "max",
        }


class TestReferenceFreeTemplates:
    """The checks that can grade production traffic, where nobody wrote an answer.

    Pinned because the property that makes them usable there is invisible: a judge
    configured with criteria grades against them, while one without falls back to
    the task's rubric and returns ungradable when there is none. A preset that
    quietly lost its criteria would still parse, still run, and report nothing.
    """

    REFERENCE_FREE = {
        "Faithfulness",
        "Hallucination",
        "Answer relevance",
        "Safety",
        "Grounded in tool results",
    }

    def _spec(self, name):
        import json

        from hopsworks_agents.eval.judge_config import default_templates

        [entry] = [t for t in default_templates() if t["name"] == name]
        return json.loads(entry["spec"])[0]

    @pytest.mark.parametrize("name", sorted(REFERENCE_FREE))
    def test_it_grades_without_an_expected_answer(self, name):
        from hopsworks_agents.eval.judge_config import parse_judge_config

        config = parse_judge_config(self._spec(name))
        # `multi` is what makes the judge grade against its own criteria rather
        # than looking for a rubric the task does not have.
        assert config.multi, f"{name} would be ungradable on production traffic"
        assert "expected_result" not in config.inputs, (
            f"{name} asks to be shown an expected answer, which production has none of"
        )

    @pytest.mark.parametrize("name", sorted(REFERENCE_FREE))
    def test_its_criteria_are_weighted_and_named(self, name):
        from hopsworks_agents.eval.judge_config import parse_judge_config

        config = parse_judge_config(self._spec(name))
        criteria = config.effective_criteria()
        assert len(criteria) > 1, f"{name} scores one thing; use a plain rubric instead"
        assert all(c.weight > 0 for c in criteria)

    def test_safety_cannot_average_away_a_leak(self):
        # A safety score that passes because two of three criteria were fine reads
        # as a pass, which is the one reading it must never produce.
        from hopsworks_agents.eval.judge_config import parse_judge_config

        config = parse_judge_config(self._spec("Safety"))
        # The floor is carried on the criterion itself, as critical_min.
        floors = {c.name: c.critical_min for c in config.effective_criteria()}
        assert floors.get("no_data_leakage")
        assert floors.get("no_harmful_content")


class TestExtraHeaders:
    def test_headers_come_off_the_entry_as_an_object_or_json_text(self):
        config = jc.parse_judge_config(
            {"provider": "openai", "headers": {"X-Tenant": "acme"}}
        )
        assert config.headers == {"X-Tenant": "acme"}
        config = jc.parse_judge_config(
            {"provider": "openai", "headers": '{"X-Route": "eu"}'}
        )
        assert config.headers == {"X-Route": "eu"}
        assert jc.parse_judge_config({"provider": "openai"}).headers == {}

    def test_malformed_headers_are_refused_with_the_reason(self):
        import pytest

        with pytest.raises(jc.JudgeConfigError, match="headers must be"):
            jc.parse_judge_config({"provider": "openai", "headers": ["X-Tenant"]})
        with pytest.raises(jc.JudgeConfigError, match="headers must be"):
            jc.parse_judge_config({"provider": "openai", "headers": {"X-Tenant": 3}})
        with pytest.raises(jc.JudgeConfigError, match="JSON object"):
            jc.parse_judge_config({"provider": "openai", "headers": "not json"})

    def test_a_dollar_value_is_read_from_the_environment_and_a_missing_one_is_not_sent(
        self,
    ):
        config = jc.JudgeConfig(
            headers={
                "Authorization": "$GATEWAY_TOKEN",
                "X-Tenant": "acme",
                "X-Missing": "$NOT_SET",
            }
        )
        resolved = jc.resolve_headers(config, environ={"GATEWAY_TOKEN": "tok"})
        assert resolved == {"Authorization": "tok", "X-Tenant": "acme"}

    def test_the_openai_client_gets_the_headers_and_base_url(self, monkeypatch):
        import sys
        import types

        seen = {}

        class FakeCompletions:
            def create(self, **kwargs):
                message = types.SimpleNamespace(content='{"score": 5}')
                return types.SimpleNamespace(
                    choices=[types.SimpleNamespace(message=message)]
                )

        class FakeOpenAI:
            def __init__(self, **options):
                seen.update(options)
                self.chat = types.SimpleNamespace(completions=FakeCompletions())

        monkeypatch.setitem(
            sys.modules, "openai", types.SimpleNamespace(OpenAI=FakeOpenAI)
        )
        monkeypatch.setenv("GATEWAY_TOKEN", "tok")
        config = jc.parse_judge_config(
            {
                "provider": "custom",
                "model": "local-llm",
                "base_url": "https://gw.internal/v1",
                "headers": {"Authorization": "$GATEWAY_TOKEN", "X-Tenant": "acme"},
            }
        )
        complete = jc.completer_for(config, "key")
        assert complete("hello") == '{"score": 5}'
        assert seen["base_url"] == "https://gw.internal/v1"
        assert seen["default_headers"] == {"Authorization": "tok", "X-Tenant": "acme"}
        assert seen["api_key"] == "key"

    def test_no_headers_means_no_default_headers_option(self, monkeypatch):
        import sys
        import types

        seen = {}

        class FakeOpenAI:
            def __init__(self, **options):
                seen.update(options)
                self.chat = types.SimpleNamespace(
                    completions=types.SimpleNamespace(
                        create=lambda **kw: types.SimpleNamespace(
                            choices=[
                                types.SimpleNamespace(
                                    message=types.SimpleNamespace(content="x")
                                )
                            ]
                        )
                    )
                )

        monkeypatch.setitem(
            sys.modules, "openai", types.SimpleNamespace(OpenAI=FakeOpenAI)
        )
        jc.completer_for(jc.parse_judge_config({"provider": "openai"}), "key")("p")
        assert "default_headers" not in seen and "base_url" not in seen
