#
#   Copyright 2024 Hopsworks AB
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

from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Any

from hopsworks_common.util import (
    FEATURE_STORE_NAME_SUFFIX,
    VALID_EMBEDDING_TYPE,
    Encoder,
    FeatureGroupWarning,
    JobWarning,
    StatisticsWarning,
    StorageWarning,
    ValidationWarning,
    VersionWarning,
    _loading_animation,
    append_feature_store_suffix,
    autofix_feature_name,
    check_timestamp_format_from_date_string,
    contains_uppercase,
    contains_whitespace,
    convert_event_time_to_timestamp,
    convert_git_status_to_files,
    convert_to_abs,
    feature_group_name,
    generate_fully_qualified_feature_name,
    get_dataset_type,
    get_delta_datestr_from_timestamp,
    get_feature_group_url,
    get_hostname_replaced_url,
    get_hudi_datestr_from_timestamp,
    get_job_url,
    get_timestamp_from_date_string,
    is_interactive,
    is_runtime_notebook,
    run_with_loading_animation,
    strip_feature_store_suffix,
    validate_embedding_feature_type,
    validate_job_conf,
    verify_attribute_key_names,
)
from hsfs import feature, serving_key
from hsfs.core import feature_group_api


if TYPE_CHECKING:
    from hsfs.constructor import filter, serving_prepared_statement


FeatureStoreEncoder = Encoder


def check_missing_mandatory_tags(
    missing_mandatory_tags: list[dict[str, Any]] | None,
    message: str = "Missing mandatory tags",
) -> None:
    if missing_mandatory_tags:
        tag_names = [tag.get("name", str(tag)) for tag in missing_mandatory_tags]
        warnings.warn(f"{message}: {tag_names}", stacklevel=2)


def validate_feature(
    ft: str | feature.Feature | dict[str, Any],
) -> feature.Feature:
    if isinstance(ft, feature.Feature):
        return ft
    if isinstance(ft, str):
        return feature.Feature(ft)
    if isinstance(ft, dict):
        return feature.Feature(**ft)
    raise TypeError("Feature must be a string, Feature object, or dictionary.")


def parse_features(
    feature_names: str | feature.Feature | list[dict[str, Any] | str | feature.Feature],
) -> list[feature.Feature]:
    if isinstance(feature_names, (str, feature.Feature)):
        return [validate_feature(feature_names)]
    if isinstance(feature_names, list) and len(feature_names) > 0:
        return [validate_feature(feat) for feat in feature_names]
    return []


_logger = logging.getLogger(__name__)

HOPS_START_TIME_ENV = "HOPS_START_TIME"
HOPS_END_TIME_ENV = "HOPS_END_TIME"


def apply_scheduler_time_defaults(
    start_time: Any,
    end_time: Any,
) -> tuple[Any, Any]:
    """Fall back to scheduler-injected `HOPS_START_TIME` / `HOPS_END_TIME` env vars.

    When a Hopsworks job is triggered by the scheduler (or via a backfill `Job.run(
    start_time=..., end_time=...)` call), the container receives `HOPS_START_TIME` and
    `HOPS_END_TIME` as ISO-8601 UTC env vars describing the data interval the run should
    process. This helper lets feature-store reads pick those up by default:

      * If the caller passed an explicit value for `start_time` / `end_time`, that value
        wins.
      * Otherwise, the corresponding env var is read and returned as-is when non-empty.
      * Empty env vars (`""`) and unset env vars are treated as "not provided" — the
        caller's `None` is preserved.

    This function does not validate the env-var string format. Any non-empty value is
    returned verbatim and is parsed downstream by `convert_event_time_to_timestamp` /
    `get_timestamp_from_date_string` at the point the time filter is built. A malformed
    env var therefore raises there, not here — with a message that names the offending
    input, which is more useful for diagnosing scheduler misconfiguration than silently
    falling back to "read whole feature group".

    Logs a one-line ``info`` message via the standard ``logging`` module when env-var
    defaults are applied so that the scheduler-injected window is visible in the
    execution log without polluting stdout (which would surface as noise in CLI
    ``--json`` output and SDK callers that capture stdout). The notice is suppressed
    when both args were explicit or when no env vars are set.

    Parameters:
        start_time: Caller-supplied start of the data window, or `None` to fall back to
            `HOPS_START_TIME`.
        end_time: Caller-supplied end of the data window, or `None` to fall back to
            `HOPS_END_TIME`.

    Returns:
        The resolved `(start_time, end_time)` tuple. Each side is the caller's value
        when it was not `None`, otherwise the corresponding env var (if set and
        non-empty), otherwise `None`.
    """
    import os

    resolved_start = start_time
    resolved_end = end_time
    applied: list[str] = []

    if start_time is None:
        env_start = os.environ.get(HOPS_START_TIME_ENV)
        if env_start:
            resolved_start = env_start
            applied.append(f"start_time={env_start} (from ${HOPS_START_TIME_ENV})")

    if end_time is None:
        env_end = os.environ.get(HOPS_END_TIME_ENV)
        if env_end:
            resolved_end = env_end
            applied.append(f"end_time={env_end} (from ${HOPS_END_TIME_ENV})")

    if applied:
        # Use ``info``-level logging instead of ``print()`` so SDK callers
        # (and CLI ``--json`` output) do not get an unexpected stdout line.
        # Job logs still surface this through the standard logging handlers.
        _logger.info("Using scheduler-injected data interval: %s", ", ".join(applied))

    return resolved_start, resolved_end


def build_time_filter(
    event_time_feature: feature.Feature,
    start_time: Any,
    end_time: Any,
) -> filter.Filter | filter.Logic | None:
    """Build a time filter from start_time and end_time parameters.

    The window is half-open `[start_time, end_time)`: `start_time` is inclusive (>=) and
    `end_time` is exclusive (<). This guarantees that back-to-back scheduled windows
    (`[t0, t1)` then `[t1, t2)`) partition the timeline — an event at exactly the boundary
    `t1` is read by the second window only, never dropped by both and never duplicated.

    Parameters:
        event_time_feature: The feature to filter on.
        start_time: The start time for the filter (inclusive, >=).
        end_time: The end time for the filter (exclusive, <).

    Returns:
        The built time filter, or `None` if both `start_time` and `end_time` are `None`.
    """
    time_filter = None
    if start_time is not None:
        time_filter = event_time_feature >= start_time

    if end_time is not None:
        end_filter = event_time_feature < end_time
        if time_filter is not None:
            time_filter = time_filter & end_filter
        else:
            time_filter = end_filter

    return time_filter


def build_serving_keys_from_prepared_statements(
    prepared_statements: list[serving_prepared_statement.ServingPreparedStatement],
    feature_store_id: int,
    ignore_prefix: bool = False,
) -> set[serving_key.ServingKey]:
    serving_keys = set()
    fg_api = feature_group_api.FeatureGroupApi()
    for statement in prepared_statements:
        fg = fg_api.get_by_id(feature_store_id, statement.feature_group_id)
        for param in statement.prepared_statement_parameters:
            serving_keys.add(
                serving_key.ServingKey(
                    feature_name=param.name,
                    join_index=statement.prepared_statement_index,
                    prefix=statement.prefix,
                    ignore_prefix=ignore_prefix,
                    feature_group=fg,
                )
            )
    return serving_keys


__all__ = [
    "FEATURE_STORE_NAME_SUFFIX",
    "VALID_EMBEDDING_TYPE",
    "Encoder",
    "FeatureStoreEncoder",
    "FeatureGroupWarning",
    "JobWarning",
    "StatisticsWarning",
    "StorageWarning",
    "ValidationWarning",
    "VersionWarning",
    "_loading_animation",
    "append_feature_store_suffix",
    "autofix_feature_name",
    "check_timestamp_format_from_date_string",
    "contains_uppercase",
    "contains_whitespace",
    "convert_event_time_to_timestamp",
    "convert_git_status_to_files",
    "convert_to_abs",
    "feature_group_name",
    "generate_fully_qualified_feature_name",
    "get_dataset_type",
    "get_delta_datestr_from_timestamp",
    "get_feature_group_url",
    "get_hostname_replaced_url",
    "get_hudi_datestr_from_timestamp",
    "get_job_url",
    "get_timestamp_from_date_string",
    "is_interactive",
    "is_runtime_notebook",
    "run_with_loading_animation",
    "strip_feature_store_suffix",
    "validate_embedding_feature_type",
    "validate_job_conf",
    "verify_attribute_key_names",
    "validate_feature",
    "parse_features",
    "build_time_filter",
    "build_serving_keys_from_prepared_statements",
]
