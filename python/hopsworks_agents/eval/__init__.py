"""Hopsworks agent evaluation: featurization, runner, and evaluators.

Runs in a Hopsworks job, never in a serving pod. The one structural rule:

    hopsworks_agents.eval  may import  hopsworks_agents.protocol
    hopsworks_agents.protocol  must never import  hopsworks_agents.eval

The serving package runs synchronously in every request; nothing in here
belongs on that path, and an import is how it would get there by accident.
``tests/test_import_isolation.py`` asserts the rule rather than trusting it.
"""

from .client import HopsworksAgentClient
from .evaluator_spec import (
    SpecError,
    evaluators_for_suite,
    evaluators_from_spec,
    validate_spec,
)
from .features import (
    TraceCompleteness,
    select_ready_traces,
    trace_features,
)
from .judge_config import JudgeConfig, JudgeConfigError, default_templates
from .judges import (
    LlmJudgeEvaluator,
    PairwiseEvaluator,
    anthropic_completer,
    pairwise_verdict,
)
from .metrics import pass_all_k, pass_at_k, run_metrics
from .models import (
    ExecutionMode,
    PassPolicy,
    Suite,
    Task,
    Trial,
    TrialStatus,
    derive_trial_id,
)
from .promotion import (
    CandidateTask,
    RedactionStatus,
    can_add_to_suite,
    confirm_redaction,
    promote_trace,
    tasks_from_trace,
)
from .runner import RunnerConfig, SuiteRefused, run_suite


__all__ = [
    "ExecutionMode",
    "HopsworksAgentClient",
    "LlmJudgeEvaluator",
    "anthropic_completer",
    "pairwise_verdict",
    "PairwiseEvaluator",
    "SpecError",
    "JudgeConfig",
    "JudgeConfigError",
    "default_templates",
    "evaluators_for_suite",
    "evaluators_from_spec",
    "validate_spec",
    "RunnerConfig",
    "PassPolicy",
    "Suite",
    "SuiteRefused",
    "Task",
    "Trial",
    "TrialStatus",
    "derive_trial_id",
    "pass_all_k",
    "pass_at_k",
    "run_metrics",
    "run_suite",
    "CandidateTask",
    "RedactionStatus",
    "TraceCompleteness",
    "can_add_to_suite",
    "confirm_redaction",
    "promote_trace",
    "select_ready_traces",
    "tasks_from_trace",
    "trace_features",
]
