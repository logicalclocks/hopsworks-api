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
from __future__ import annotations

from typing import Any

from hopsworks_common.core.ontology_api import MAX_ROWS
from pydantic import BaseModel, Field


class OntologyAnswer(BaseModel):
    """The result of one question to an ontology system."""

    answer: str = Field(description="The answer in plain language.")
    sql: str = Field(default="", description="The SQL that produced the rows.")
    rows: list[dict[str, Any]] = Field(
        default_factory=list, description=f"The result rows, at most {MAX_ROWS}."
    )
    row_count: int = Field(
        default=0, description="How many rows the query returned before the cut."
    )
    truncated: bool = Field(
        default=False, description=f"True when more than {MAX_ROWS} rows matched."
    )
    reasoning: str = Field(
        default="", description="What the agent did, one line per step."
    )
    chart: Any = Field(
        default=None,
        description="The chart or table the app saved with the turn; only in a conversation.",
    )


class OntologyReadiness(BaseModel):
    """Whether an ontology system can answer questions."""

    ready: bool = Field(
        description="True when a question can be understood and its SQL run on data."
    )
    ontology_compiled: bool | None = Field(
        default=False,
        description="Whether the ontology is compiled; null when the caller may not see it.",
    )
    can_execute_sql: bool | None = Field(
        default=False,
        description="Whether a data source is configured to run SQL on; null when it could not be checked.",
    )
    catalog_present: bool | None = Field(
        default=False,
        description="Whether any database is ingested; null when it could not be checked.",
    )
    databases: list[str] = Field(
        default_factory=list, description="The databases in the catalog."
    )
    blockers: list[str] = Field(
        default_factory=list,
        description="What stands in the way, and what to do about it; empty when ready.",
    )
    unverified: list[str] = Field(
        default_factory=list,
        description="Facts the caller could not check; they do not make the system unready.",
    )
