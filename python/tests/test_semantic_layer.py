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

import pytest
from hopsworks_common import semantic_layer
from hopsworks_common.semantic_layer import (
    SemanticBuild,
    SemanticGroupBy,
    SemanticIssue,
    SemanticMetric,
    SemanticProject,
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
        assert sp.current_build_id is None
        assert sp.missing_mandatory_tags == []
        assert SemanticProject.from_response_json(None) is None

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


def test_polars_conversion_of_a_json_result():
    pl = pytest.importorskip("polars")

    frame = semantic_layer._to_dataframe(
        {"columns": [{"name": "a"}, {"name": "b"}], "rows": [[1, "x"], [2, None]]},
        "polars",
    )

    assert isinstance(frame, pl.DataFrame)
    assert frame.columns == ["a", "b"]
    assert frame["a"].to_list() == [1, 2]
