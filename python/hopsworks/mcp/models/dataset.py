#
#   Copyright 2025 Hopsworks AB
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

from pydantic import BaseModel


class Dataset(BaseModel):
    """Model representing a dataset in Hopsworks MCP."""

    id: int
    name: str
    description: str | None = None
    datasetType: str


class Datasets(BaseModel):
    """Model representing a collection of datasets in Hopsworks MCP."""

    datasets: list[Dataset]
    total: int
    offset: int
    limit: int


class File(BaseModel):
    """Model representing a file in a dataset."""

    name: str
    is_directory: bool
    owner: str
    path: str
    permission: str
    under_construction: bool | None = None
    last_modified: str | None = None


class Files(BaseModel):
    """Model representing a collection of files in a dataset."""

    files: list[File]
    total: int
    offset: int
    limit: int
