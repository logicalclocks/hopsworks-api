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

"""Unit tests for the `partitioned_by` transform grammar and per-format rules."""

import pytest
from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs.core import partition_transforms
from hsfs.core.partition_transforms import PartitionTransform


# region — parse_expression


class TestParseExpression:
    @pytest.mark.parametrize(
        ("expr", "expected"),
        [
            ("identity(col)", PartitionTransform("identity", "col")),
            ("some_col", PartitionTransform("identity", "some_col")),
            ("bucket(16, cc_num)", PartitionTransform("bucket", "cc_num", 16)),
            ("truncate(4, zip)", PartitionTransform("truncate", "zip", 4)),
            ("year(ts)", PartitionTransform("year", "ts")),
            ("month(ts)", PartitionTransform("month", "ts")),
            ("week(ts)", PartitionTransform("week", "ts")),
            ("day(ts)", PartitionTransform("day", "ts")),
            ("hour(ts)", PartitionTransform("hour", "ts")),
            ("void(col)", PartitionTransform("void", "col")),
        ],
    )
    def test_parses_every_transform(self, expr, expected):
        assert partition_transforms._parse_expression(expr) == expected

    def test_whitespace_tolerant(self):
        assert partition_transforms._parse_expression(
            "  bucket( 16 ,  cc_num )  "
        ) == PartitionTransform("bucket", "cc_num", 16)

    def test_transform_name_case_insensitive(self):
        assert partition_transforms._parse_expression("DAY(ts)") == PartitionTransform(
            "day", "ts"
        )

    def test_bare_column_is_identity(self):
        t = partition_transforms._parse_expression("region")
        assert t.name == "identity"
        assert t.source == "region"
        assert t.param is None

    @pytest.mark.parametrize("grain", ["year", "month", "week", "day", "hour", "YEAR"])
    def test_bare_grain_rejected_with_migration_hint(self, grain):
        with pytest.raises(FeatureStoreException, match="removed grain form") as e:
            partition_transforms._parse_expression(grain)
        # the hint spells out the new transform form on the event_time column
        assert f"{grain.lower()}(event_time_col)" in str(e.value)

    @pytest.mark.parametrize("grain", ["year", "month", "week", "day", "hour"])
    def test_identity_on_grain_named_column_round_trips(self, grain):
        # A bare grain name is rejected as legacy syntax, so the canonical
        # form must keep the explicit identity() for these sources.
        t = partition_transforms._parse_expression(f"identity({grain})")
        assert str(t) == f"identity({grain})"
        assert partition_transforms._parse_expression(str(t)) == t

    def test_identity_on_ordinary_column_canonicalizes_bare(self):
        assert (
            str(partition_transforms._parse_expression("identity(region)")) == "region"
        )

    @pytest.mark.parametrize(
        ("expr", "expected_source"),
        [
            ("day(Event_TS)", "event_ts"),
            ("bucket(16, CustomerId)", "customerid"),
            ("Region", "region"),
            ("identity(Region)", "region"),
        ],
    )
    def test_source_canonicalized_to_lower_case(self, expr, expected_source):
        # Feature names are sanitized to lower case; day(Event_TS) must not pass
        # validation and then fail the case-sensitive Iceberg binding.
        assert partition_transforms._parse_expression(expr).source == expected_source

    def test_sort_order_column_canonicalized_to_lower_case(self):
        fields = partition_transforms._parse_sort_order(["Amount desc"])
        assert fields[0].column == "amount"
        # and case-only duplicates are then caught
        with pytest.raises(FeatureStoreException, match="more than once"):
            partition_transforms._parse_sort_order(["amount asc", "Amount desc"])

    @pytest.mark.parametrize("expr", ["", "   ", None, 16])
    def test_non_string_or_empty_rejected(self, expr):
        with pytest.raises(FeatureStoreException, match="non-empty string"):
            partition_transforms._parse_expression(expr)

    @pytest.mark.parametrize("expr", ["day(ts", "bucket 16, id)", "a.b", "f(g(x))"])
    def test_unparseable_expression_rejected(self, expr):
        with pytest.raises(FeatureStoreException, match="Cannot parse"):
            partition_transforms._parse_expression(expr)

    def test_unknown_transform_rejected(self):
        with pytest.raises(FeatureStoreException, match="Unknown partition transform"):
            partition_transforms._parse_expression("minute(ts)")

    @pytest.mark.parametrize("expr", ["bucket(16)", "bucket(16, a, b)", "truncate(id)"])
    def test_parameterized_transform_argument_count(self, expr):
        with pytest.raises(FeatureStoreException, match="takes two arguments"):
            partition_transforms._parse_expression(expr)

    @pytest.mark.parametrize(
        "expr", ["bucket(0, id)", "bucket(-1, id)", "bucket(x, id)", "truncate(1.5, s)"]
    )
    def test_parameter_must_be_positive_integer(self, expr):
        with pytest.raises(FeatureStoreException, match="positive integer"):
            partition_transforms._parse_expression(expr)

    @pytest.mark.parametrize("expr", ["day(a, b)", "identity()", "year()"])
    def test_unparameterized_transform_argument_count(self, expr):
        with pytest.raises(FeatureStoreException, match="takes one column argument"):
            partition_transforms._parse_expression(expr)

    @pytest.mark.parametrize("expr", ["day(a b)", "bucket(16, a-b)"])
    def test_bad_column_name_rejected(self, expr):
        with pytest.raises(FeatureStoreException, match="Invalid column name"):
            partition_transforms._parse_expression(expr)

    @pytest.mark.parametrize(
        ("expr", "expected"),
        [
            ("region", "region"),
            ("identity(col)", "col"),
            ("bucket(16, id)", "id_bucket"),
            ("truncate(4, zip)", "zip_trunc"),
            ("void(x)", "x_null"),
            ("year(ts)", "ts_year"),
            ("month(ts)", "ts_month"),
            ("week(ts)", "ts_week"),
            ("day(ts)", "ts_day"),
            ("hour(ts)", "ts_hour"),
            ("bucket(16, id) as shard", "shard"),
        ],
    )
    def test_field_name_alias_or_iceberg_default(self, expr, expected):
        assert partition_transforms._parse_expression(expr).field_name == expected


# endregion


# region — parse / try_parse


class TestParse:
    def test_parse_preserves_order(self):
        transforms = partition_transforms._parse(
            ["year(ts)", "bucket(16, id)", "region"]
        )
        assert [t.name for t in transforms] == ["year", "bucket", "identity"]

    def test_duplicates_rejected(self):
        with pytest.raises(FeatureStoreException, match="duplicate transform"):
            partition_transforms._parse(["day(ts)", "day( ts )"])

    def test_same_transform_different_param_is_not_a_duplicate(self):
        # not a duplicate transform, but both default to the generated
        # partition field name "id_bucket", so the names collide ...
        with pytest.raises(
            FeatureStoreException, match="produces the partition field name"
        ):
            partition_transforms._parse(["bucket(4,id)", "bucket(8,id)"])
        # ... and an alias on one of them resolves the collision
        transforms = partition_transforms._parse(
            ["bucket(4,id)", "bucket(8,id) as id_bucket8"]
        )
        assert [t.field_name for t in transforms] == ["id_bucket", "id_bucket8"]

    def test_repeated_alias_rejected(self):
        with pytest.raises(
            FeatureStoreException, match="produces the partition field name"
        ):
            partition_transforms._parse(["day(ts) as shard", "bucket(16,id) as shard"])

    def test_alias_colliding_with_generated_default_rejected(self):
        with pytest.raises(
            FeatureStoreException, match="produces the partition field name"
        ):
            partition_transforms._parse(["bucket(16,id) as ts_day", "day(ts)"])

    def test_try_parse_none_and_empty(self):
        assert partition_transforms._try_parse(None) is None
        assert partition_transforms._try_parse([]) is None

    def test_try_parse_old_grain_form_returns_none(self):
        assert partition_transforms._try_parse(["year", "month"]) is None

    def test_try_parse_garbage_returns_none(self):
        assert partition_transforms._try_parse(["day(ts"]) is None

    def test_try_parse_valid_spec(self):
        transforms = partition_transforms._try_parse(["day(ts)", "bucket(16, id)"])
        assert transforms == [
            PartitionTransform("day", "ts"),
            PartitionTransform("bucket", "id", 16),
        ]


# endregion


# region — per-format validation matrix


def _parse(exprs):
    return partition_transforms._parse(exprs)


class TestValidateForFormat:
    def test_iceberg_allows_all_but_week(self):
        transforms = _parse(
            [
                "id",
                "bucket(16, id)",
                "truncate(4, zip)",
                "void(x)",
                "year(a)",
                "month(b)",
                "day(c)",
                "hour(d)",
            ]
        )
        partition_transforms._validate_for_format(transforms, "ICEBERG", ["id"], "ts")

    def test_iceberg_rejects_week(self):
        with pytest.raises(
            FeatureStoreException, match="not supported for time_travel_format=ICEBERG"
        ):
            partition_transforms._validate_for_format(
                _parse(["week(ts)"]), "ICEBERG", ["id"], "ts"
            )

    def test_iceberg_rejects_redundant_temporal_on_same_source(self):
        with pytest.raises(FeatureStoreException, match="at most one temporal"):
            partition_transforms._validate_for_format(
                _parse(["year(ts)", "day(ts)"]), "ICEBERG", ["id"], "ts"
            )

    def test_iceberg_allows_temporal_on_distinct_sources(self):
        partition_transforms._validate_for_format(
            _parse(["year(created)", "day(updated)"]), "ICEBERG", ["id"], "ts"
        )

    def test_delta_rejects_partitioned_by(self):
        # Delta has no partition transforms: liquid clustering goes through
        # clustered_by and identity partitions through partition_key
        with pytest.raises(FeatureStoreException, match="not supported on DELTA") as e:
            partition_transforms._validate_for_format(
                _parse(["day(ts)"]), "DELTA", ["id"], "ts"
            )
        assert "clustered_by" in str(e.value)
        assert "partition_key" in str(e.value)

    def test_delta_rejected_even_for_identity_transforms(self):
        with pytest.raises(FeatureStoreException, match="not supported on DELTA"):
            partition_transforms._validate_for_format(
                _parse(["id"]), "DELTA", ["id"], "ts"
            )

    def test_allowed_transforms_native_formats_only(self):
        # partitioned_by describes native partition specs; only Iceberg and
        # Hudi have them
        assert set(partition_transforms.ALLOWED_TRANSFORMS) == {"ICEBERG", "HUDI"}

    @pytest.mark.parametrize("expr", ["truncate(4, zip)", "void(x)", "bucket(16, id)"])
    def test_hudi_rejects_non_native_transforms(self, expr):
        # the Hudi bucket index goes through bucket_index, not a transform
        with pytest.raises(
            FeatureStoreException, match="not supported for time_travel_format=HUDI"
        ):
            partition_transforms._validate_for_format(
                _parse([expr]), "HUDI", ["id"], "ts"
            )

    def test_hudi_rejects_alias(self):
        # Hudi has no partition-field naming; aliases are Iceberg-only
        with pytest.raises(FeatureStoreException, match="Iceberg-only"):
            partition_transforms._validate_for_format(
                _parse(["region as r"]), "HUDI", ["id"], "ts"
            )

    def test_hudi_temporal_requires_event_time(self):
        with pytest.raises(FeatureStoreException, match="requires event_time"):
            partition_transforms._validate_for_format(
                _parse(["day(ts)"]), "HUDI", ["id"], None
            )

    def test_hudi_temporal_source_must_be_event_time(self):
        with pytest.raises(FeatureStoreException, match="event_time column"):
            partition_transforms._validate_for_format(
                _parse(["day(created)"]), "HUDI", ["id"], "ts"
            )

    def test_hudi_accepts_temporal_on_event_time(self):
        partition_transforms._validate_for_format(
            _parse(["year(ts)", "month(ts)", "week(ts)"]), "HUDI", ["id"], "ts"
        )

    def test_hudi_accepts_identity_with_temporal(self):
        partition_transforms._validate_for_format(
            _parse(["region", "day(ts)"]), "HUDI", ["id"], "ts"
        )

    @pytest.mark.parametrize("fmt", [None, "NONE", "unknown"])
    def test_unsupported_format_rejected(self, fmt):
        with pytest.raises(FeatureStoreException, match="requires time_travel_format"):
            partition_transforms._validate_for_format(
                _parse(["day(ts)"]), fmt, ["id"], "ts"
            )

    def test_format_is_case_insensitive(self):
        partition_transforms._validate_for_format(
            _parse(["day(ts)"]), "iceberg", ["id"], "ts"
        )


# endregion


# region — helpers


class TestHelpers:
    def test_temporal_grains_in_order(self):
        transforms = _parse(["day(ts)", "bucket(16, id)", "year(created)"])
        assert partition_transforms._temporal_grains(transforms) == ["day", "year"]

    def test_identity_columns(self):
        transforms = _parse(["region", "identity(country)", "day(ts)"])
        assert partition_transforms._identity_columns(transforms) == [
            "region",
            "country",
        ]

    def test_serialize_canonical_form(self):
        transforms = _parse(
            ["  DAY( ts )", "bucket( 16 , id )", "identity(region)", "Truncate(4,zip)"]
        )
        assert partition_transforms._serialize(transforms) == [
            "day(ts)",
            "bucket(16,id)",
            "region",
            "truncate(4,zip)",
        ]

    def test_parse_round_trips_canonical_form(self):
        canonical = ["day(ts)", "bucket(16,id)", "region", "void(x)", "hour(t2)"]
        assert (
            partition_transforms._serialize(partition_transforms._parse(canonical))
            == canonical
        )


# endregion
