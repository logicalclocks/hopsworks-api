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

import datetime
import json
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from hopsworks_common import user as user_mod
from hopsworks_common import util
from hsfs.core.constants import HAS_GREAT_EXPECTATIONS


if TYPE_CHECKING or HAS_GREAT_EXPECTATIONS:
    from great_expectations.core import (
        ExpectationSuite,
        ExpectationSuiteValidationResult,
    )
    from hsfs import expectation_suite as es_mod
    from hsfs import validation_report as vr_mod


class FeatureStoreActivityType(Enum):
    METADATA = "METADATA"
    STATISTICS = "STATISTICS"
    JOB = "JOB"
    VALIDATIONS = "VALIDATIONS"
    EXPECTATIONS = "EXPECTATIONS"
    COMMIT = "COMMIT"


@dataclass(frozen=True, init=False, repr=False)
class FeatureStoreActivity:
    type: FeatureStoreActivityType
    metadata: str
    timestamp: int
    user: user_mod.User
    # optional fields depending on the activity type
    validation_report: Optional[
        Union[vr_mod.ValidationReport, ExpectationSuiteValidationResult]
    ] = None
    expectation_suite: Optional[Union[es_mod.ExpectationSuite, ExpectationSuite]] = None
    commit: Optional[Dict[str, Union[str, int, float]]] = None
    statistics: Optional[Dict[str, Union[str, int, float]]] = None
    # internal fields
    id: int
    href: str

    def __init__(
        self,
        type: FeatureStoreActivityType,
        metadata: str,
        timestamp: int,
        user: Dict[str, Any],
        **kwargs,
    ):
        self.type = type
        self.metadata = metadata
        self.timestamp = timestamp
        self.user = user_mod.User.from_response_json(user)

        self.id = kwargs.get("id")
        self.href = kwargs.get("href", "")

        self.commit = None
        self.expectation_suite = None
        self.validation_report = None
        self.statistics = None

        if self.type == FeatureStoreActivityType.VALIDATIONS:
            if HAS_GREAT_EXPECTATIONS:
                self.validation_report = ExpectationSuiteValidationResult(
                    **kwargs.get("validation_report")
                )
            else:
                self.validation_report = vr_mod.ValidationReport(
                    **kwargs.get("validation_report")
                )

        if self.type == FeatureStoreActivityType.EXPECTATIONS:
            if HAS_GREAT_EXPECTATIONS:
                self.expectation_suite = ExpectationSuite(
                    **kwargs.get("expectation_suite")
                )
            else:
                self.expectation_suite = es_mod.ExpectationSuite(
                    **kwargs.get("expectation_suite")
                )

        if self.type == FeatureStoreActivityType.COMMIT:
            self.commit = kwargs.get("commit")

        if self.statistics:
            self.statistics = kwargs.get("statistics")

    @classmethod
    def from_response_json(
        cls, response_json: Dict[str, Any]
    ) -> List[FeatureStoreActivity]:
        if "items" in response_json:
            return [
                cls.from_response_json(activity) for activity in response_json["items"]
            ]
        else:
            return cls(**response_json)

    def to_dict(self) -> Dict[str, Any]:
        json = {
            "id": self.id,
            "type": self.type.value,
            "metadata": self.metadata,
            "timestamp": self.timestamp,
            "user": self.user.json(),
        }
        if self.validation_report:
            json["validation_report"] = self.validation_report.json()
        if self.expectation_suite:
            json["expectation_suite"] = self.expectation_suite.json()
        if self.commit:
            json["commit"] = self.commit
        if self.statistics:
            json["statistics"] = self.statistics

        return json

    def json(self) -> str:
        return json.dumps(self.to_dict(), cls=util.Encoder)

    def __repr__(self):
        utc_human_readable = (
            datetime.datetime.fromtimestamp(
                self.timestamp, datetime.timezone.utc
            ).strftime(r"%Y-%m-%d %H:%M:%S")
            + " UTC"
        )
        return f"Activity:\n{self.type},\n{self.metadata},\nat: {utc_human_readable},\nuser: {self.user}"
