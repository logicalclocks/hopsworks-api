"""The agent-serving client: everything the Hopsworks UI does for agents, from Python.

import hopsworks
from hopsworks_agents.eval.sdk import check

agents = hopsworks.login().get_agent_serving()
agent = agents.get_agent("support")
print(agent.chat("hello").text)
for cluster in agent.clusters(): ...
"""

from ._transport import AgentServingError
from .agent import Agent, ChatStream, StreamFrame
from .client import AgentServing
from .evals import check
from .models import (
    Calibration,
    ChatReply,
    Check,
    Cluster,
    EvalJob,
    EvaluatorResult,
    EvaluatorTemplate,
    Feedback,
    FeedbackPage,
    FeedbackSummary,
    GateCheck,
    GateResult,
    LlmMetric,
    RegressionSuite,
    ReviewJob,
    Run,
    RunMetric,
    Suite,
    Task,
    ToolEvent,
    ToolMetric,
    Trace,
    TraceMetric,
    TraceSummary,
    Triage,
    Trial,
)


__all__ = [
    "Agent",
    "AgentServing",
    "AgentServingError",
    "Calibration",
    "ChatReply",
    "ChatStream",
    "Check",
    "Cluster",
    "EvalJob",
    "EvaluatorResult",
    "EvaluatorTemplate",
    "Feedback",
    "FeedbackPage",
    "FeedbackSummary",
    "GateCheck",
    "GateResult",
    "LlmMetric",
    "RegressionSuite",
    "ReviewJob",
    "Run",
    "RunMetric",
    "Suite",
    "StreamFrame",
    "Task",
    "ToolEvent",
    "ToolMetric",
    "Trace",
    "TraceMetric",
    "TraceSummary",
    "Trial",
    "Triage",
    "check",
]
