"""Promotion, and the gate that makes it safe.

Promotion copies production user data into a long-lived suite, outside normal
retention: a later deletion request removes the trace and leaves the copy. The
tests that matter are therefore the refusals, not the happy path.
"""

import json

import pytest
from hopsworks_agents.eval.promotion import (
    RedactionStatus,
    can_add_to_suite,
    confirm_redaction,
    promote_trace,
    tasks_from_trace,
)


def trace(**overrides):
    row = {
        "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
        "deployment_id": 7,
        "input_messages": json.dumps(
            [{"role": "user", "content": "which albums did I like?"}]
        ),
        "final_output": "In Through The Out Door and Led Zeppelin III",
        "tool_names": json.dumps(["recall", "purchase_history"]),
    }
    row.update(overrides)
    return row


class TestTheGate:
    def test_a_freshly_promoted_task_cannot_join_a_suite(self):
        # the whole safety property in one assertion
        task = promote_trace(trace(), task_id="task-1")
        allowed, reason = can_add_to_suite(task)
        assert allowed is False
        assert "PENDING_REDACTION" in reason

    def test_clean_content_still_requires_review(self):
        # nothing was flagged, but the detectors cannot see a customer's name in
        # free text -- letting a clean scan through unreviewed is exactly the
        # hole the gate exists to close
        task = promote_trace(trace(), task_id="task-1")
        assert task.findings == []
        assert task.redaction_status is RedactionStatus.PENDING_REDACTION
        assert can_add_to_suite(task)[0] is False

    def test_a_confirmed_task_may_join_a_suite(self):
        task = promote_trace(trace(), task_id="task-1")
        confirm_redaction(task, reviewer="gibson@logicalclocks.com")
        assert can_add_to_suite(task) == (True, "")

    def test_confirmation_without_a_reviewer_is_refused(self):
        # an unattributed confirmation is the same as no confirmation
        task = promote_trace(trace(), task_id="task-1")
        with pytest.raises(ValueError):
            confirm_redaction(task, reviewer="")

    def test_a_redacted_task_with_no_reviewer_cannot_join_a_suite(self):
        # guards against the status being set directly, bypassing confirm
        task = promote_trace(trace(), task_id="task-1")
        task.redaction_status = RedactionStatus.REDACTED
        allowed, reason = can_add_to_suite(task)
        assert allowed is False
        assert "reviewer" in reason

    def test_the_refusal_explains_itself(self):
        task = promote_trace(trace(), task_id="task-1")
        allowed, reason = can_add_to_suite(task)
        assert not allowed and task.task_id in reason


class TestRedactionOnPromotion:
    def test_secrets_in_the_input_are_flagged_and_removed(self):
        task = promote_trace(
            trace(
                input_messages=json.dumps(
                    [{"role": "user", "content": "my email is a@b.com"}]
                )
            ),
            task_id="task-1",
        )
        assert [f["kind"] for f in task.findings] == ["EMAIL"]
        assert "a@b.com" not in task.input_messages
        assert "[EMAIL]" in task.input_messages

    def test_secrets_in_the_proposed_output_are_flagged(self):
        task = promote_trace(
            trace(final_output="your key is AKIAIOSFODNN7EXAMPLE"),
            task_id="task-1",
        )
        assert any(f["kind"] == "AWS_ACCESS_KEY" for f in task.findings)
        assert "AKIAIOSFODNN7EXAMPLE" not in task.proposed_output

    def test_findings_record_the_field_they_came_from(self):
        task = promote_trace(
            trace(
                input_messages=json.dumps([{"role": "user", "content": "a@b.com"}]),
                final_output="10.0.0.5",
            ),
            task_id="task-1",
        )
        by_field = {f["field"]: f["kind"] for f in task.findings}
        assert by_field == {"input_messages": "EMAIL", "proposed_output": "IP_ADDRESS"}

    def test_findings_never_carry_the_secret_itself(self):
        task = promote_trace(
            trace(final_output="key AKIAIOSFODNN7EXAMPLE"), task_id="task-1"
        )
        assert all("AKIAIOSFODNN7EXAMPLE" not in json.dumps(f) for f in task.findings)

    def test_auto_redaction_can_be_deferred_to_the_reviewer(self):
        task = promote_trace(
            trace(final_output="key AKIAIOSFODNN7EXAMPLE"),
            task_id="task-1",
            auto_redact=False,
        )
        assert task.findings, "detection must still run"
        assert "AKIAIOSFODNN7EXAMPLE" in task.proposed_output

    def test_a_reviewer_can_edit_the_content_while_confirming(self):
        task = promote_trace(trace(), task_id="task-1")
        confirm_redaction(
            task, reviewer="gibson@logicalclocks.com", expected_output="corrected"
        )
        assert task.proposed_output == "corrected"
        assert task.redaction_status is RedactionStatus.REDACTED


class TestTaskContent:
    def test_the_correction_is_used_as_the_proposal(self):
        # promotion usually comes from negative feedback: the corrected answer
        # is the point, not what the agent actually said
        task = promote_trace(
            trace(), task_id="task-1", expected_output="Led Zeppelin III only"
        )
        assert task.proposed_output == "Led Zeppelin III only"

    def test_without_a_correction_it_falls_back_to_the_agents_answer(self):
        task = promote_trace(trace(), task_id="task-1")
        assert task.proposed_output.startswith("In Through The Out Door")

    def test_tools_the_trace_used_become_required_tools(self):
        task = promote_trace(trace(), task_id="task-1")
        assert task.required_tools == ["recall", "purchase_history"]

    def test_malformed_tool_names_do_not_break_promotion(self):
        task = promote_trace(trace(tool_names="not json"), task_id="task-1")
        assert task.required_tools == []

    def test_the_row_records_where_it_came_from(self):
        row = promote_trace(trace(), task_id="task-1").to_row()
        assert row["source_trace_id"] == "4bf92f3577b34da6a3ce929d0e0e4736"
        assert row["source_deployment_id"] == 7
        assert row["redaction_status"] == "PENDING_REDACTION"


class TestPurgePath:
    def test_tasks_can_be_found_by_the_trace_they_came_from(self):
        # the deletion-request story: the trace goes, and so must everything
        # copied out of it
        tasks = [
            promote_trace(trace(), task_id="task-1"),
            promote_trace(trace(trace_id="other"), task_id="task-2"),
            promote_trace(trace(), task_id="task-3"),
        ]
        found = tasks_from_trace(tasks, "4bf92f3577b34da6a3ce929d0e0e4736")
        assert {t.task_id for t in found} == {"task-1", "task-3"}
