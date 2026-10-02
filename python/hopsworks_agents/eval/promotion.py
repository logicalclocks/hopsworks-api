"""Turn a production trace into an eval task.

The highest-risk flow in the design, and the risk is not technical. Promotion
copies production user data into a long-lived suite, which crosses a retention
boundary: a later deletion request removes the trace and leaves the copy. So
promotion is two steps, not one.

1. A candidate task is created ``PENDING_REDACTION``, with automatic detection
   results attached.
2. A human confirms or edits the redacted content before the task can join a
   suite.

The gate is the whole point. Detection raises the floor; it cannot find a
customer's name in free text, and treating it as sufficient would make the
review theatre. :func:`can_add_to_suite` is therefore the function to reach
for, and it refuses by default.

``source_trace_id`` is recorded on every promoted task so a deletion request
can find and purge what came from a given trace — the one thing that makes the
retention boundary crossable at all.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

from hopsworks_agents.protocol import detectors


if TYPE_CHECKING:
    from collections.abc import Sequence


class RedactionStatus(str, Enum):
    # nothing sensitive was found *and* nothing needed removing; still the
    # result of a scan, not an assumption
    NOT_REQUIRED = "NOT_REQUIRED"
    PENDING_REDACTION = "PENDING_REDACTION"
    REDACTED = "REDACTED"


# Only these may join a suite. Written as a set rather than "not PENDING" so
# that a status added later is excluded until someone decides otherwise.
SUITE_ELIGIBLE = {RedactionStatus.NOT_REQUIRED, RedactionStatus.REDACTED}


@dataclass
class CandidateTask:
    """A task proposed from a trace, before review."""

    task_id: str
    source_trace_id: str
    source_deployment_id: int
    task_type: str
    input_messages: str
    #: What the agent actually answered, proposed as an expectation.
    #:
    #: A suggestion, not a stored expectation: a promoted task has no suite yet,
    #: so there is no check to key an expectation to. It becomes one when the
    #: task joins a suite and someone says which check it answers.
    proposed_output: str
    redaction_status: RedactionStatus
    findings: list[dict[str, Any]] = field(default_factory=list)
    required_tools: list[str] = field(default_factory=list)
    category: str = ""
    tags: list[str] = field(default_factory=list)
    reviewer: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    def to_row(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "task_version": 1,
            "task_type": self.task_type,
            "input_messages": self.input_messages,
            # Carried alongside the task rather than as a column on it: the
            # backend stores expectations per check, and this one answers none
            # yet.
            "proposed_output": self.proposed_output,
            "proposed_tools": json.dumps(self.required_tools),
            "source_trace_id": self.source_trace_id,
            "source_deployment_id": self.source_deployment_id,
            "redaction_status": self.redaction_status.value,
            "category": self.category,
            "tags": json.dumps(self.tags),
            "reviewer": self.reviewer,
            "created_at": self.created_at,
        }


def _finding_dicts(
    findings: Sequence[detectors.Finding], field_name: str
) -> list[dict]:
    # `preview` rather than the value: a review UI showing the findings would
    # otherwise re-expose the very secrets being redacted
    return [
        {
            "field": field_name,
            "kind": finding.kind,
            "preview": finding.preview,
            "start": finding.start,
            "end": finding.end,
        }
        for finding in findings
    ]


def promote_trace(
    trace_features: dict[str, Any],
    *,
    task_id: str,
    expected_output: str | None = None,
    category: str = "",
    tags: Sequence[str] | None = None,
    auto_redact: bool = True,
) -> CandidateTask:
    """Build a candidate task from one ``agent_trace_features`` row.

    ``expected_output`` is the reviewer's corrected answer when promotion came
    from negative feedback. Falling back to the agent's own ``final_output``
    is deliberate but rarely what you want: it makes the task assert that the
    agent should keep doing exactly what it did, which is right for a
    ``golden`` task and wrong for the regression case that produced most
    promotions. Callers should pass the correction.
    """
    input_messages = trace_features.get("input_messages") or "[]"
    answer = (
        expected_output
        if expected_output is not None
        else (trace_features.get("final_output") or "")
    )

    findings = _finding_dicts(detectors.detect(input_messages), "input_messages")
    findings += _finding_dicts(detectors.detect(answer), "proposed_output")

    if findings and auto_redact:
        input_messages = detectors.redact(input_messages)
        answer = detectors.redact(answer)

    # Even a clean scan lands PENDING when it came from real traffic: the
    # detectors cannot see a customer's name in free text, and NOT_REQUIRED
    # would let that through unreviewed. Only content nothing flagged *and*
    # that no human data touched can skip review, which promotion never is.
    status = RedactionStatus.PENDING_REDACTION

    required_tools = []
    raw_tools = trace_features.get("tool_names")
    if raw_tools:
        try:
            required_tools = (
                json.loads(raw_tools) if isinstance(raw_tools, str) else list(raw_tools)
            )
        except (json.JSONDecodeError, TypeError):
            required_tools = []

    return CandidateTask(
        task_id=task_id,
        source_trace_id=trace_features.get("trace_id", ""),
        source_deployment_id=trace_features.get("deployment_id", 0),
        task_type="single_turn",
        input_messages=input_messages,
        proposed_output=answer,
        redaction_status=status,
        findings=findings,
        required_tools=required_tools,
        category=category,
        tags=list(tags or []),
    )


def confirm_redaction(
    task: CandidateTask,
    *,
    reviewer: str,
    input_messages: str | None = None,
    expected_output: str | None = None,
) -> CandidateTask:
    """Record a human's confirmation, optionally with edits.

    The reviewer is required and recorded: the point of the gate is that a
    named person looked, and an unattributed confirmation is the same as no
    confirmation.
    """
    if not reviewer:
        raise ValueError("confirm_redaction requires a reviewer")
    if input_messages is not None:
        task.input_messages = input_messages
    if expected_output is not None:
        task.proposed_output = expected_output
    task.reviewer = reviewer
    task.redaction_status = RedactionStatus.REDACTED
    return task


def can_add_to_suite(task: CandidateTask) -> tuple[bool, str]:
    """Whether this task may join a suite, and why not if it may not.

    Returns a reason rather than a bare False so the refusal can be shown to
    the person it is refusing.
    """
    if task.redaction_status not in SUITE_ELIGIBLE:
        return False, (
            f"task {task.task_id} is {task.redaction_status.value}: promoted "
            "content must be reviewed before it can join a suite"
        )
    if task.redaction_status is RedactionStatus.REDACTED and not task.reviewer:
        return False, (
            f"task {task.task_id} is marked REDACTED but records no reviewer"
        )
    return True, ""


def tasks_from_trace(
    tasks: Sequence[CandidateTask], trace_id: str
) -> list[CandidateTask]:
    """Every task promoted from a given trace.

    The purge path for a deletion request: the trace goes, and so must
    everything copied out of it. Cheap to provide, and impossible to
    reconstruct later if ``source_trace_id`` was not recorded at promotion.
    """
    return [task for task in tasks if task.source_trace_id == trace_id]
