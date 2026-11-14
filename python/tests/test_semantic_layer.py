#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

import datetime
import json

import pytest
from hopsworks_common import semantic_layer
from hopsworks_common.semantic_layer import (
    SemanticBuild,
    SemanticExport,
    SemanticGroupBy,
    SemanticIssue,
    SemanticMetric,
    SemanticProject,
    SemanticSavedQuery,
    _query_request,
)


class TestGroupByParsing:
    @pytest.mark.parametrize(
        ("name", "ref"),
        [
            ("metric_time__month", {"name": "metric_time", "grain": "month"}),
            ("booking__ds__WEEK", {"name": "booking__ds", "grain": "week"}),
            ("listing__country_latest", {"name": "listing__country_latest"}),
            ("ds", {"name": "ds"}),
            ("metric_time__extract_dow", {"name": "metric_time", "datePart": "dow"}),
            # Not a grain: sent whole, for the gateway to resolve.
            ("metric_time__fiscal_quarter", {"name": "metric_time__fiscal_quarter"}),
        ],
    )
    def test_qualified_names(self, name, ref):
        assert _query_request(["m"], group_by=[name])["groupBy"] == [ref]

    def test_dicts_take_snake_or_camel_case(self):
        request = _query_request(
            ["m"],
            group_by=[
                {"name": "metric_time", "grain": "MONTH"},
                {"name": "metric_time", "date_part": "doy"},
                {"name": "metric_time", "datePart": "dow"},
            ],
        )

        assert request["groupBy"] == [
            {"name": "metric_time", "grain": "month"},
            {"name": "metric_time", "datePart": "doy"},
            {"name": "metric_time", "datePart": "dow"},
        ]

    def test_a_single_string_is_one_group_by(self):
        assert _query_request("m", group_by="customer__country") == {
            "metrics": ["m"],
            "groupBy": [{"name": "customer__country"}],
        }


class TestQueryRequest:
    def test_only_given_arguments_are_sent(self):
        assert _query_request(["revenue"]) == {"metrics": ["revenue"]}

    def test_every_field(self):
        request = _query_request(
            ["revenue", "order_count"],
            group_by=["metric_time__month", "customer__country"],
            where="{{ Dimension('customer__country') }} != 'XX'",
            filters=[
                ("customer__country", "in", ("SE", "US")),
                {"group_by": "order__status", "op": "IS NOT NULL"},
                ("metric_time__day", ">=", "2026-01-01"),
            ],
            having=[
                ("revenue", ">", 10),
                {"metric": "order_count", "op": "<", "value": 5},
            ],
            order_by=[
                "-revenue",
                "metric_time__month",
                {"name": "order_count", "descending": True},
            ],
            limit=100,
            offset=20,
            time_range=(datetime.date(2026, 1, 1), "2026-02-01"),
            group_limit={
                "group_by": "customer__country",
                "top": 5,
                "metric": "revenue",
            },
        )

        assert request == {
            "metrics": ["revenue", "order_count"],
            "groupBy": [
                {"name": "metric_time", "grain": "month"},
                {"name": "customer__country"},
            ],
            "where": ["{{ Dimension('customer__country') }} != 'XX'"],
            "filters": [
                {
                    "groupBy": {"name": "customer__country"},
                    "op": "IN",
                    "value": ["SE", "US"],
                },
                {"groupBy": {"name": "order__status"}, "op": "IS NOT NULL"},
                {
                    "groupBy": {"name": "metric_time", "grain": "day"},
                    "op": ">=",
                    "value": "2026-01-01",
                },
            ],
            "having": [
                {"metric": "revenue", "op": ">", "value": 10},
                {"metric": "order_count", "op": "<", "value": 5},
            ],
            "orderBy": [
                {"name": "revenue", "descending": True},
                {"name": "metric_time__month", "descending": False},
                {"name": "order_count", "descending": True},
            ],
            "limit": 100,
            "offset": 20,
            "timeRange": {"start": "2026-01-01", "end": "2026-02-01"},
            "groupLimit": {
                "groupBy": [{"name": "customer__country"}],
                "top": 5,
                "metric": "revenue",
                "descending": True,
            },
        }

    def test_an_open_time_range_drops_the_missing_bound(self):
        start = datetime.datetime(2026, 1, 1, 12, 30)

        assert _query_request(["m"], time_range={"start": start})["timeRange"] == {
            "start": "2026-01-01T12:30:00"
        }
        assert _query_request(["m"], time_range=(None, "2026-02-01"))["timeRange"] == {
            "end": "2026-02-01"
        }

    def test_timezone_aware_datetimes_keep_their_offset(self):
        utc = datetime.datetime(
            2026, 1, 1, 0, 0, 0, 123456, tzinfo=datetime.timezone.utc
        )
        cet = datetime.datetime(
            2026, 1, 1, 1, tzinfo=datetime.timezone(datetime.timedelta(hours=1))
        )

        request = _query_request(
            ["m"],
            filters=[
                ("order__order_ts__microsecond", "=", utc),
                ("order__order_ts__hour", "IN", [cet, "2026-01-02T00:00:00Z"]),
            ],
            time_range=(utc, cet),
        )

        assert request["timeRange"] == {
            "start": "2026-01-01T00:00:00.123456+00:00",
            "end": "2026-01-01T01:00:00+01:00",
        }
        assert [f["value"] for f in request["filters"]] == [
            "2026-01-01T00:00:00.123456+00:00",
            ["2026-01-01T01:00:00+01:00", "2026-01-02T00:00:00Z"],
        ]
        json.dumps(request)


class TestSavedQueryRequest:
    def test_a_saved_query_is_sent_without_metrics(self):
        request = _query_request(
            None,
            saved_query="revenue_by_country",
            group_by="metric_time__month",
            where="{{ Dimension('customer__country') }} = 'SE'",
            filters=[("order__status", "=", "paid")],
            order_by="-revenue",
            limit=10,
        )

        assert request == {
            "savedQuery": "revenue_by_country",
            "groupBy": [{"name": "metric_time", "grain": "month"}],
            "where": ["{{ Dimension('customer__country') }} = 'SE'"],
            "filters": [
                {"groupBy": {"name": "order__status"}, "op": "=", "value": "paid"}
            ],
            "orderBy": [{"name": "revenue", "descending": True}],
            "limit": 10,
        }

    def test_a_saved_query_alone_sends_only_its_name(self):
        assert _query_request(None, saved_query="revenue_by_country") == {
            "savedQuery": "revenue_by_country"
        }

    @pytest.mark.parametrize("metrics", [["revenue"], "revenue"])
    def test_metrics_with_a_saved_query_are_refused(self, metrics):
        with pytest.raises(ValueError, match="leave out metrics"):
            _query_request(metrics, saved_query="revenue_by_country")

    def test_empty_metrics_go_with_a_saved_query(self):
        assert _query_request([], saved_query="revenue_by_country") == {
            "savedQuery": "revenue_by_country"
        }

    def test_an_empty_group_by_asks_for_totals_and_an_empty_order_by_drops_the_sort(
        self,
    ):
        assert _query_request(
            None, saved_query="revenue_by_country", group_by=[], order_by=[]
        ) == {"savedQuery": "revenue_by_country", "groupBy": [], "orderBy": []}

    def test_without_metrics_or_a_saved_query_the_metrics_are_empty(self):
        assert _query_request(None, group_by="customer__country") == {
            "metrics": [],
            "groupBy": [{"name": "customer__country"}],
        }


class TestModels:
    def test_project_round_trip(self):
        sp = SemanticProject.from_response_json(
            {
                "id": 7,
                "projectId": 119,
                "name": "sales",
                "rootPath": "/Projects/demo/dbt",
                "format": "DBT",
                "deploymentMode": "FOLLOW",
                "currentBuildId": None,
                "latestBuild": {"id": 1, "status": "COMPILING", "gitCommit": "a1b2c3d"},
                "someNewField": "ignored",
            }
        )

        assert sp.latest_build == SemanticBuild(
            id=1, status="COMPILING", git_commit="a1b2c3d"
        )
        assert sp.to_dict()["latest_build"]["git_commit"] == "a1b2c3d"
        assert list(sp.to_dict()) == [
            "id",
            "project_id",
            "name",
            "root_path",
            "git_repository_id",
            "format",
            "deployment_mode",
            "current_build_id",
            "latest_build",
            "creator",
            "owner",
            "created",
            "missing_mandatory_tags",
        ]
        assert sp.current_build_id is None
        assert sp.missing_mandatory_tags == []
        assert SemanticProject.from_response_json(None) is None

    def test_project_reads_its_owner_and_keeps_its_creator(self):
        sp = SemanticProject.from_response_json(
            {"id": 7, "name": "sales", "creator": "meb10000", "owner": "meb10001"}
        )

        assert (sp.creator, sp.owner) == ("meb10000", "meb10001")
        assert sp.to_dict()["owner"] == "meb10001"
        assert sp.to_dict()["creator"] == "meb10000"

    def test_a_project_without_an_owner(self):
        sp = SemanticProject.from_response_json(
            {"id": 7, "name": "sales", "creator": "meb10000", "owner": None}
        )

        assert sp.owner is None
        assert sp.to_dict()["owner"] is None

    def test_missing_mandatory_tags_are_their_names(self):
        sp = SemanticProject.from_response_json(
            {"id": 7, "name": "sales", "missingMandatoryTags": [{"name": "owner"}]}
        )

        assert sp.missing_mandatory_tags == ["owner"]
        assert sp.to_dict()["missing_mandatory_tags"] == ["owner"]

    def test_build_with_issues(self):
        build = SemanticBuild.from_response_json(
            {
                "id": 12,
                "semanticProjectId": 7,
                "status": "INVALID",
                "errorCount": 1,
                "warningCount": 0,
                "issues": [
                    {
                        "level": "ERROR",
                        "rule": "UNIQUE_NAMES",
                        "message": "dup",
                        "file": "a.yml",
                        "line": 3,
                    }
                ],
            }
        )

        assert build.semantic_project_id == 7
        assert build.issues == [
            SemanticIssue(
                level="ERROR", rule="UNIQUE_NAMES", message="dup", file="a.yml", line=3
            )
        ]
        assert str(build.issues[0]) == "ERROR UNIQUE_NAMES a.yml:3: dup"
        assert (
            SemanticBuild.from_response_json(build.to_dict() | {"issues": None}).id
            == 12
        )

    def test_metric_and_group_by(self):
        metric = SemanticMetric.from_response_json(
            {"name": "aov", "type": "RATIO", "agg": None, "requiresMetricTime": False}
        )
        group_by = SemanticGroupBy.from_response_json(
            {
                "name": "listing__country_latest",
                "type": "CATEGORICAL",
                "entityPath": ["listing"],
                "dimension": "country_latest",
                "datePartsSupported": False,
            }
        )

        assert metric.requires_metric_time is False
        assert metric.to_dict()["agg"] is None
        assert group_by.entity_path == ["listing"]
        assert group_by.date_parts_supported is False
        assert group_by.event_time is False
        order_ts = SemanticGroupBy.from_response_json(
            {"name": "order__order_ts", "type": "TIME", "eventTime": True}
        )
        assert order_ts.event_time is True
        assert order_ts.to_dict()["event_time"] is True

    def test_metric_tags_keep_the_case_of_schema_names_and_value_keys(self):
        metric = SemanticMetric.from_response_json(
            {
                "name": "revenue",
                "type": "SIMPLE",
                "tags": {"dataOwnership": {"ownerTeam": "finance", "certified": True}},
            }
        )

        assert metric.tags == {
            "dataOwnership": {"ownerTeam": "finance", "certified": True}
        }
        assert metric.to_dict()["tags"] == metric.tags

    def test_a_metric_without_tags_has_an_empty_mapping(self):
        assert SemanticMetric.from_response_json({"name": "orders"}).tags == {}
        assert SemanticMetric(name="orders").tags == {}


class TestSavedQuery:
    JSON = {
        "name": "revenue_by_country",
        "label": "Revenue by country",
        "description": "Monthly revenue per country",
        "metrics": ["revenue", "orders"],
        "groupBy": ["metric_time__month", "customer__country"],
        "where": ["{{ Dimension('customer__country') }} != 'XX'"],
        "orderBy": [
            {"name": "metric_time__month"},
            {"name": "revenue", "descending": True},
        ],
        "limit": 10,
        "tags": {"dataOwner": {"ownerTeam": "finance"}},
        "exports": [{"name": "ignored"}],
    }

    def test_from_the_gateway_answer(self):
        saved_query = SemanticSavedQuery.from_response_json(self.JSON)

        assert saved_query == SemanticSavedQuery(
            name="revenue_by_country",
            label="Revenue by country",
            description="Monthly revenue per country",
            metrics=["revenue", "orders"],
            group_by=["metric_time__month", "customer__country"],
            where=["{{ Dimension('customer__country') }} != 'XX'"],
            order_by=[
                {"name": "metric_time__month", "descending": False},
                {"name": "revenue", "descending": True},
            ],
            limit=10,
            tags={"dataOwner": {"ownerTeam": "finance"}},
        )
        assert saved_query.to_dict()["group_by"] == [
            "metric_time__month",
            "customer__country",
        ]

    def test_missing_and_null_lists_are_empty(self):
        saved_query = SemanticSavedQuery.from_response_json(
            {"name": "q", "metrics": ["revenue"], "groupBy": None}
        )

        assert saved_query.group_by == []
        assert saved_query.where == []
        assert saved_query.order_by == []
        assert saved_query.tags == {}
        assert SemanticSavedQuery.from_response_json(None) is None


class TestExport:
    JSON = {
        "name": "revenue_daily",
        "savedQuery": "revenue_by_country",
        "exportAs": "TABLE",
        "featureGroup": "revenue_daily",
        "featureGroupId": 501,
        "version": 1,
        "schedule": "0 0 2 * * ?",
        "lookback": "7 days",
        "enabled": True,
        "jobName": "semantic_export_sales_revenue_daily",
        "lastStatus": "SUCCEEDED",
        "lastRun": "2026-11-07T02:00:05Z",
        "rows": 123,
        "lastMessage": "Replaced the rows from 2026-10-31T00:00:00Z on.",
        "generation": 3,
    }

    def test_from_the_control_plane_answer(self):
        export = SemanticExport.from_response_json(self.JSON)

        assert export == SemanticExport(
            name="revenue_daily",
            saved_query="revenue_by_country",
            export_as="TABLE",
            feature_group="revenue_daily",
            feature_group_id=501,
            version=1,
            schedule="0 0 2 * * ?",
            lookback="7 days",
            enabled=True,
            job_name="semantic_export_sales_revenue_daily",
            last_status="SUCCEEDED",
            last_run="2026-11-07T02:00:05Z",
            last_rows=123,
            last_message="Replaced the rows from 2026-10-31T00:00:00Z on.",
        )
        assert SemanticExport.from_response_json(export.to_dict()) == export

    def test_an_export_that_never_ran(self):
        export = SemanticExport.from_response_json(
            {"name": "e", "lastStatus": "NEVER_RUN", "featureGroupId": None}
        )

        assert export.feature_group_id is None
        assert export.last_rows is None
        assert export.last_status == "NEVER_RUN"
        assert SemanticExport.from_response_json(None) is None


def test_polars_conversion_of_a_json_result():
    pl = pytest.importorskip("polars")

    frame = semantic_layer._to_dataframe(
        {"columns": [{"name": "a"}, {"name": "b"}], "rows": [[1, "x"], [2, None]]},
        "polars",
    )

    assert isinstance(frame, pl.DataFrame)
    assert frame.columns == ["a", "b"]
    assert frame["a"].to_list() == [1, 2]
