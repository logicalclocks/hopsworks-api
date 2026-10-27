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

from abc import ABC, abstractmethod
from typing import TypeVar

from hopsworks_apigen import public


@public
class FeatureLogger(ABC):
    @public
    def log_batch(
        self, payload: bytes, attributes: dict[str, str], rows: int = 1
    ) -> None:
        """Submit one Arrow IPC feature batch to a capable inference logger.

        Implementations opt in by overriding this method. Callers must also
        check the injector's protocol capability before submitting a batch.

        Parameters:
            payload: One uncompressed Arrow IPC stream containing one batch.
            attributes: Binary CloudEvent headers, including revision identity.
            rows: Number of rows already known by the batch builder.

        Raises:
            NotImplementedError: This logger supports row logging only.
        """
        raise NotImplementedError("This feature logger does not support Arrow batches")

    @public
    @abstractmethod
    def log(
        self,
        untransformed_features: list[dict] | None = None,
        transformed_features: list[dict] | None = None,
    ):
        pass

    @public
    @abstractmethod
    def init(self, feature_view: TypeVar("hsfs.feature_view.FeatureView")) -> None:
        pass
