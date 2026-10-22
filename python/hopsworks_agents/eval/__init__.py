"""Hopsworks agent evaluation: featurization, runner, and evaluators.

Runs in a Hopsworks job, never in a serving pod. The one structural rule:

    hopsworks_agents.eval  may import  hopsworks_agents.protocol
    hopsworks_agents.protocol  must never import  hopsworks_agents.eval

The serving package runs synchronously in every request; nothing in here
belongs on that path, and an import is how it would get there by accident.
``tests/test_import_isolation.py`` asserts the rule rather than trusting it.

Internal. What a user is meant to call lives in ``hopsworks_agents.eval.sdk``,
reached through ``project.get_agent_serving()``; the runner, the evaluators,
the judges and the job modules in here are how that is carried out. Only the
types and the exceptions those interfaces hand back are re-exported -- a
private function has no business in a package's ``__all__``.
"""

from .client import HopsworksAgentClient
from .evaluator_spec import SpecError
from .features import TraceCompleteness
from .judge_config import JudgeConfig, JudgeConfigError
from .judges import LlmJudgeEvaluator, PairwiseEvaluator
from .models import (
    ExecutionMode,
    PassPolicy,
    Suite,
    Task,
    Trial,
    TrialStatus,
)
from .promotion import CandidateTask, RedactionStatus
from .runner import RunnerConfig, SuiteRefused


__all__ = [
    "CandidateTask",
    "ExecutionMode",
    "HopsworksAgentClient",
    "JudgeConfig",
    "JudgeConfigError",
    "LlmJudgeEvaluator",
    "PairwiseEvaluator",
    "PassPolicy",
    "RedactionStatus",
    "RunnerConfig",
    "SpecError",
    "Suite",
    "SuiteRefused",
    "Task",
    "TraceCompleteness",
    "Trial",
    "TrialStatus",
]
