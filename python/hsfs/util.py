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

from typing import TYPE_CHECKING, Any, Dict, List, Set, Union

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
    from hsfs.constructor import serving_prepared_statement


FeatureStoreEncoder = Encoder


def validate_feature(
    ft: Union[str, feature.Feature, Dict[str, Any]],
) -> feature.Feature:
    if isinstance(ft, feature.Feature):
        return ft
    elif isinstance(ft, str):
        return feature.Feature(ft)
    elif isinstance(ft, dict):
        return feature.Feature(**ft)


def parse_features(
    feature_names: Union[
        str, feature.Feature, List[Union[Dict[str, Any], str, feature.Feature]]
    ],
) -> List[feature.Feature]:
    if isinstance(feature_names, (str, feature.Feature)):
        return [validate_feature(feature_names)]
    elif isinstance(feature_names, list) and len(feature_names) > 0:
        return [validate_feature(feat) for feat in feature_names]
    else:
        return []


def build_serving_keys_from_prepared_statements(
    prepared_statements: List[serving_prepared_statement.ServingPreparedStatement],
    feature_store_id: int,
    ignore_prefix: bool = False,
) -> Set[serving_key.ServingKey]:
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
    "build_serving_keys_from_prepared_statements",
]
