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

from pydantic import BaseModel


class SemanticProject(BaseModel):
    """A semantic project and the build that answers its queries."""

    name: str
    root_path: str | None = None
    deployment_mode: str | None = None
    serving_build_id: int | None = None
    stale: bool | None = None
    not_serving: str | None = None


class Tag(BaseModel):
    """A schematised tag: its schema name and its value."""

    name: str
    value: Any = None


class SemanticIssue(BaseModel):
    """A validation error or warning of a build, at its file and line."""

    level: str | None = None
    rule: str | None = None
    message: str | None = None
    file: str | None = None
    line: int | None = None
    column: int | None = None
    element: str | None = None


class SemanticBuild(BaseModel):
    """The outcome of a compile: VALID builds serve queries, INVALID and FAILED ones never do."""

    build_id: int | None = None
    status: str | None = None
    error_count: int | None = None
    warning_count: int | None = None
    issues: list[SemanticIssue] = []


class Dashboard(BaseModel):
    """A Superset dashboard: its id, its title, and the URL that opens it through Hopsworks."""

    id: int
    title: str | None = None
    url: str
