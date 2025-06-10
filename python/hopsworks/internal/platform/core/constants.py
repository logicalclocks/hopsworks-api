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

import importlib.util


# Avro
HAS_FAST_AVRO: bool = importlib.util.find_spec("fastavro") is not None
HAS_AVRO: bool = importlib.util.find_spec("avro") is not None
avro_not_installed_message = (
    "Avro package not found. "
    "If you want to use avro with Hopsworks you can install the corresponding extra via "
    '`pip install "hopsworks[avro]"`. '
    "You can also install avro directly in your environment with `pip install fastavro` or `pip install avro`. "
    "You will need to restart your kernel if applicable."
)

# Confluent Kafka
HAS_CONFLUENT_KAFKA: bool = importlib.util.find_spec("confluent_kafka") is not None
confluent_kafka_not_installed_message = (
    "Confluent Kafka package not found. "
    "If you want to use Kafka with Hopsworks you can install the corresponding extras via "
    '`pip install "hopsworks[python]"`. '
    "You can also install confluent-kafka directly in your environment with `pip install confluent-kafka`. "
    "You will need to restart your kernel if applicable."
)

# Data Validation / Great Expectations
HAS_GREAT_EXPECTATIONS: bool = (
    importlib.util.find_spec("great_expectations") is not None
)
great_expectations_not_installed_message = (
    "Great Expectations package not found. "
    "If you want to use data validation with Hopsworks you can install the corresponding extras via "
    '`pip install "hopsworks[great_expectations]"`. '
    "You can also install great-expectations directly in your environment with `pip install great-expectations`. "
    "You will need to restart your kernel if applicable."
)
initialise_expectation_suite_for_single_expectation_api_message = "Initialize Expectation Suite by attaching to a Feature Group to enable single expectation API"

# Pyarrow
HAS_PYARROW: bool = importlib.util.find_spec("pyarrow") is not None
pyarrow_not_installed_message = (
    "Pyarrow package not found. "
    "If you want to use Apache Arrow with Hopsworks you can install the corresponding extras via "
    '`pip install "hopsworks[python]"`. '
    "You can also install pyarrow directly in your environment with `pip install pyarrow`. "
    "You will need to restart your kernel if applicable."
)

HAS_PANDAS: bool = importlib.util.find_spec("pandas") is not None

# NumPy
HAS_NUMPY: bool = importlib.util.find_spec("numpy") is not None
numpy_not_installed_message = (
    "Numpy package not found. "
    "If you want to use numpy with Hopsworks you can install the corresponding extra via "
    '`pip install "hopsworks[numpy]"`. '
    "You can also install numpy directly in your environment with `pip install numpy`. "
    "You will need to restart your kernel if applicable."
)

HAS_POLARS: bool = importlib.util.find_spec("polars") is not None
polars_not_installed_message = (
    "Polars package not found. "
    "If you want to use Polars with Hopsworks you can install the corresponding extra via "
    """'`pip install "hopsworks[polars]"`. '"""
    "You can also install polars directly in your environment with `pip install polars`. "
    "You will need to restart your kernel if applicable."
)

# SQL packages
HAS_SQLALCHEMY: bool = importlib.util.find_spec("sqlalchemy") is not None
HAS_AIOMYSQL: bool = importlib.util.find_spec("aiomysql") is not None
