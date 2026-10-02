from datetime import datetime, timezone

from hopsworks_agents.eval import clusters as c


def row(
    i,
    signature="discount not applied",
    *,
    severity="medium",
    status="usable",
    confidence=0.5,
    ungradable=False,
    created="2026-09-10T08:00:0{}Z",
):
    return {
        "feedback_id": f"fb-{i}",
        "trace_id": f"trace-{i}",
        "deployment_id": 3,
        "failure_signature": signature,
        "failure_summary": f"summary {i}",
        "severity": severity,
        "correction_status": status,
        "confidence": confidence,
        "ungradable": ungradable,
        "created_at": created.format(i),
    }


class TestAssigning:
    def test_the_same_signature_is_one_cluster_and_rows_point_at_it(self):
        rows = [row(0), row(1), row(2, "wrong tool")]
        changed = c.assign_clusters(rows, [])
        by_sig = {k.signature: k for k in changed}
        assert set(by_sig) == {"discount not applied", "wrong tool"}
        assert by_sig["discount not applied"].size == 2
        assert (
            rows[0]["cluster_id"]
            == rows[1]["cluster_id"]
            == by_sig["discount not applied"].cluster_id
        )
        assert rows[2]["cluster_id"] == by_sig["wrong tool"].cluster_id
        assert by_sig["discount not applied"].first_seen == datetime(
            2026, 9, 10, 8, 0, 0, tzinfo=timezone.utc
        )
        assert by_sig["discount not applied"].last_seen == datetime(
            2026, 9, 10, 8, 0, 1, tzinfo=timezone.utc
        )

    def test_an_existing_cluster_grows_instead_of_being_duplicated(self):
        existing = c.Cluster(
            cluster_id="c-old",
            deployment_id=3,
            signature="discount not applied",
            size=9,
            severity="low",
            representative_feedback_id="fb-old",
            first_seen=datetime(2026, 9, 1, tzinfo=timezone.utc),
        )
        rows = [row(5, severity="high")]
        changed = c.assign_clusters(rows, [existing])
        assert [k.cluster_id for k in changed] == ["c-old"]
        assert existing.size == 10
        assert existing.severity == "high"
        assert existing.first_seen == datetime(2026, 9, 1, tzinfo=timezone.utc)
        assert rows[0]["cluster_id"] == "c-old"

    def test_the_representative_is_the_best_member_and_only_moves_for_a_better_one(
        self,
    ):
        rows = [
            row(0, severity="low", status="missing", confidence=0.9),
            row(1, severity="high", status="usable", confidence=0.4),
            row(2, severity="high", status="usable", confidence=0.3),
        ]
        (cluster,) = c.assign_clusters(rows, [])
        assert cluster.representative_feedback_id == "fb-1"
        assert cluster.label == "summary 1"
        # an existing representative keeps its place against an equal or weaker newcomer
        existing = c.from_api(
            {
                "clusterId": "c",
                "deploymentId": 3,
                "signature": "s",
                "size": 3,
                "severity": "high",
                "representativeFeedbackId": "fb-rep",
            }
        )
        c.assign_clusters([row(7, "s", severity="high", status="missing")], [existing])
        assert existing.representative_feedback_id == "fb-rep"
        c.assign_clusters(
            [row(8, "s", severity="critical", status="usable")], [existing]
        )
        assert existing.representative_feedback_id == "fb-8"

    def test_unclustered_rows_say_so_rather_than_joining_an_unknown_bucket(self):
        rows = [row(0, ungradable=True), row(1, signature="")]
        assert c.assign_clusters(rows, []) == []
        assert rows[0]["cluster_id"] == "" and rows[1]["cluster_id"] == ""

    def test_a_dismissed_cluster_counts_its_new_members_but_stays_dismissed(self):
        existing = c.Cluster(
            cluster_id="c", deployment_id=3, signature="s", size=1, status="dismissed"
        )
        c.assign_clusters([row(0, "s")], [existing])
        assert existing.size == 2 and existing.status == "dismissed"


class TestKnownSignatures:
    def test_largest_live_clusters_first_without_the_dismissed(self):
        clusters = [
            c.Cluster("a", 3, "small", size=1),
            c.Cluster("b", 3, "big", size=40),
            c.Cluster("d", 3, "gone", size=100, status="dismissed"),
            c.Cluster("e", 3, "big", size=2),
        ]
        assert c.known_signatures(clusters) == ["big", "small"]
        assert c.known_signatures(clusters, limit=1) == ["big"]


class TestRows:
    def test_a_row_has_every_column_and_no_null_timestamp(self):
        now = datetime(2026, 9, 10, tzinfo=timezone.utc)
        fresh = c.Cluster(cluster_id="c", deployment_id=3, signature="s").to_row(now)
        assert (
            fresh["first_seen"] == now
            and fresh["last_seen"] == now
            and fresh["updated_at"] == now
        )
        assert fresh["decided_at"] == "" and fresh["status"] == "open"
        assert set(fresh) >= {
            "cluster_id",
            "deployment_id",
            "signature",
            "label",
            "severity",
            "size",
            "representative_trace_id",
            "representative_feedback_id",
            "promoted_task_id",
            "dismiss_reason",
            "decided_by",
        }

    def test_api_shape_round_trips(self):
        parsed = c.from_api(
            {
                "clusterId": "c",
                "deploymentId": "3",
                "signature": "s",
                "size": "4",
                "firstSeen": "2026-09-01T00:00:00Z",
                "lastSeen": 1_789_000_000_000,
                "status": "promoted",
                "promotedTaskId": "t",
            }
        )
        assert parsed.size == 4 and parsed.deployment_id == 3
        assert parsed.first_seen == datetime(2026, 9, 1, tzinfo=timezone.utc)
        assert parsed.last_seen.tzinfo is not None
        assert parsed.status == "promoted" and parsed.promoted_task_id == "t"
