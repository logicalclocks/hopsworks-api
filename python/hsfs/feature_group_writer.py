#
#   Copyright 2023 Hopsworks AB
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

from typing import TYPE_CHECKING, Any, TypeVar


if TYPE_CHECKING:
    from hopsworks_common.core.constants import HAS_NUMPY

    if HAS_NUMPY:
        import numpy as np
    import pandas as pd
    from hsfs.core.job import Job
    from hsfs.validation_report import ValidationReport


class FeatureGroupWriter:
    def __init__(self, feature_group):
        self._feature_group = feature_group

    def __enter__(self):
        return self

    def insert(
        self,
        features: pd.DataFrame
        | TypeVar("pyspark.sql.DataFrame")
        | TypeVar("pyspark.RDD")
        | np.ndarray
        | list[list],
        overwrite: bool | None = False,
        operation: str | None = "upsert",
        storage: str | None = None,
        write_options: dict[str, Any] | None = None,
        validation_options: dict[str, Any] | None = None,
        transformation_context: dict[str, Any] = None,
        transform: bool = True,
    ) -> tuple[Job | None, ValidationReport | None]:
        if validation_options is None:
            validation_options = {}
        if write_options is None:
            write_options = {}
        return self._feature_group.insert(
            features=features,
            overwrite=overwrite,
            operation=operation,
            storage=storage,
            write_options={"start_offline_materialization": False, **write_options},
            validation_options={"fetch_expectation_suite": False, **validation_options},
            transformation_context=transformation_context,
            transform=transform,
        )

    def __exit__(self, exc_type, exc_value, exc_tb):
        self._feature_group.finalize_multi_part_insert()
