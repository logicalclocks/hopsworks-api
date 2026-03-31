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

from hopsworks_apigen import also_available_as
from hopsworks_common import util


@also_available_as(
    "hopsworks.core.ingestion_job_conf.IngestionJobConf",
    "hsfs.core.ingestion_job_conf.IngestionJobConf",
)
class IngestionJobConf:
    def __init__(
        self, data_format, data_options, write_options, spark_job_configuration
    ):
        self.data_format = data_format
        self.data_options = data_options
        self.write_options = write_options
        self.spark_job_configuration = spark_job_configuration

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        return {
            "dataFormat": self.data_format,
            "dataOptions": self.data_options,
            "writeOptions": [
                {"name": k, "value": v} for k, v in self.write_options.items()
            ]
            if self.write_options
            else None,
            "sparkJobConfiguration": self.spark_job_configuration,
        }
