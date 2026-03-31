#
#   Copyright 2020 Logical Clocks AB
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

import json

import humps
from hsfs import util


class FeatureGroupCommit:
    NOT_FOUND_ERROR_CODE = 270227

    def __init__(
        self,
        commitid=None,
        commit_date_string=None,
        rows_inserted=None,
        rows_updated=None,
        rows_deleted=None,
        validation_id=None,
        commit_time=None,
        archived=None,
        last_active_commit_time=None,
        table_size=None,
        items=None,
        count=None,
        href=None,
        **kwargs,
    ):
        self.commitid = commitid
        self._commit_date_string = commit_date_string
        self.commit_time = commit_time
        self.rows_inserted = rows_inserted
        self.rows_updated = rows_updated
        self.rows_deleted = rows_deleted
        self.validation_id = validation_id
        self._archived = archived
        self.last_active_commit_time = last_active_commit_time
        self.table_size = table_size

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return [cls(**commit_dto) for commit_dto in json_decamelized["items"]]

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("href")
        self.__init__(**json_decamelized)
        return self

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        return {
            "commitID": self.commitid,
            "commitDateString": self._commit_date_string,
            "commitTime": self.commit_time,
            "rowsInserted": self.rows_inserted,
            "rowsUpdated": self.rows_updated,
            "rowsDeleted": self.rows_deleted,
            "validationId": self.validation_id,
            "archived": self._archived,
            "lastActiveCommitTime": self.last_active_commit_time,
            "tableSize": self.table_size,
        }

    @property
    def commit_date_string(self):
        return self._commit_date_string

    @property
    def archived(self):
        return self._archived
