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

from hopsworks_apigen import public


if TYPE_CHECKING:
    from hopsworks_common.core.constants import HAS_NUMPY

    if HAS_NUMPY:
        import numpy as np
    import pandas as pd
    from hsfs.core.job import Job
    from hsfs.validation_report import ValidationReport


@public
class FeatureGroupWriter:
    """Context manager for optimized multi-part inserts into a feature group.

    Acquired through [`FeatureGroup.multi_part_insert`][hsfs.feature_group.FeatureGroup.multi_part_insert].
    Within the `with` block, repeated [`insert`][hsfs.feature_group_writer.FeatureGroupWriter.insert]
    calls batch many small Dataframes and transmit them efficiently; the
    materialization job only starts once, when the context exits.
    """

    def __init__(self, feature_group):
        self._feature_group = feature_group

    def __enter__(self):
        return self

    @public
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
        """Insert one batch of a multi-part insert into the feature group.

        Batches are buffered and transmitted efficiently; offline
        materialization is deferred until the surrounding context exits.

        Parameters:
            features: Features to be inserted in this batch.
            overwrite: Drop all data in the feature group before inserting, without affecting metadata.
            operation: Apache Hudi operation type, `"insert"` or `"upsert"`.
            storage: Restrict the write to `"offline"` or `"online"` storage; defaults to both.
            write_options: Additional write options as key-value pairs.
            validation_options: Additional data-validation options as key-value pairs.
            transformation_context: Context variables passed to the feature group's transformation functions.
            transform: Whether to apply the feature group's on-demand transformations before writing.

        Returns:
            A tuple of the materialization job (if any) and the validation report (if data validation ran).
        """
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
