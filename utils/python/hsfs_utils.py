from __future__ import annotations

import sys
import os
import argparse
import json
from datetime import datetime
from typing import Any, Dict
import traceback

import fsspec.implementations.arrow as pfs

hopsfs = pfs.HadoopFileSystem("default", user=os.environ["HADOOP_USER_NAME"])
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, _parse_datatype_string
from pyspark.sql.functions import max, expr

import hopsworks

from hsfs import engine
from hsfs.constructor import query
from hsfs.statistics_config import StatisticsConfig
from hsfs.core import feature_monitoring_config_engine, feature_view_engine, kafka_engine


def read_job_conf(path: str) -> Dict[Any, Any]:
    """
    The configuration file is passed as path on HopsFS
    The path is a JSON containing different values depending on the op type
    """
    file_name = os.path.basename(path)
    hopsfs.download(path, ".")
    with open(file_name, "r") as f:
        return json.loads(f.read())


def setup_spark() -> SparkSession:
    return SparkSession.builder.enableHiveSupport().getOrCreate()


def get_feature_store_handle(feature_store: str = "") -> hsfs.feature_store:
    project = hopsworks.login()
    return project.get_feature_store(feature_store)


def sort_schema(fg_schema: StructType, csv_df_schema: StructType) -> StructType:
    # The schema order of the fg_schema needs to match the
    # order of the csv_df_schema
    csv_df_schema_indices = [
        csv_df_schema.names.index(field) for field in fg_schema.names
    ]
    fg_schema_sorted = sorted(
        zip(fg_schema.fields, csv_df_schema_indices), key=lambda x: x[1]
    )
    return StructType([f[0] for f in fg_schema_sorted])


def get_fg_spark_df(job_conf: Dict[Any, Any], fg_schema: StructType) -> Any:
    data_path = job_conf.pop("data_path")
    data_format = job_conf.pop("data_format")
    data_options = job_conf.pop("data_options")

    csv_df = spark.read.format(data_format).options(**data_options).load(data_path)

    schema = sort_schema(fg_schema, csv_df.schema)

    return (
        spark.read.format(data_format)
        .options(**data_options)
        .schema(schema)
        .load(data_path)
    )


def insert_fg(spark: SparkSession, job_conf: Dict[Any, Any]) -> None:
    """
    Insert data into a feature group.
    The data path, feature group name and versions are in the configuration file
    """
    feature_store = job_conf.pop("feature_store")
    fs = get_feature_store_handle(feature_store)

    fg = fs.get_feature_group(name=job_conf["name"], version=job_conf["version"])

    schema = StructType(
        [StructField(f.name, _parse_datatype_string(f.type), True) for f in fg.features]
    )

    df = get_fg_spark_df(job_conf, schema)
    fg.insert(df, write_options=job_conf.pop("write_options", {}) or {})


def create_td(job_conf: Dict[Any, Any]) -> None:
    # Extract the feature store handle
    feature_store = job_conf.pop("feature_store")
    fs = get_feature_store_handle(feature_store)

    # Extract the query object
    q = query.Query._hopsworks_json(job_conf.pop("query"))

    td = fs.get_training_dataset(name=job_conf["name"], version=job_conf["version"])
    td.insert(
        q,
        overwrite=job_conf.pop("overwrite", False) or False,
        write_options=job_conf.pop("write_options", {}) or {},
    )


def create_fv_td(job_conf: Dict[Any, Any]) -> None:
    # Extract the feature store handle
    feature_store = job_conf.pop("feature_store")
    fs = get_feature_store_handle(feature_store)

    fv = fs.get_feature_view(name=job_conf["name"], version=job_conf["version"])
    fv_engine = feature_view_engine.FeatureViewEngine(fv.featurestore_id)

    user_write_options = job_conf.pop("write_options", {}) or {}

    training_helper_columns = user_write_options.get("training_helper_columns")
    primary_keys = user_write_options.get("primary_keys")
    event_time = user_write_options.get("event_time")
    fv_engine.compute_training_dataset(
        feature_view_obj=fv,
        user_write_options=user_write_options,
        primary_keys=primary_keys,
        event_time=event_time,
        training_helper_columns=training_helper_columns,
        training_dataset_version=job_conf["td_version"],
    )


def compute_stats(job_conf: Dict[Any, Any]) -> None:
    """
    Compute/Update statistics on a feature group
    """
    feature_store = job_conf.pop("feature_store")
    fs = get_feature_store_handle(feature_store)

    entity_type = job_conf["type"]
    if entity_type == "fg":
        entity = fs.get_feature_group(
            name=job_conf["name"], version=job_conf["version"]
        )
    elif entity_type == "external_fg":
        entity = fs.get_external_feature_group(
            name=job_conf["name"], version=job_conf["version"]
        )
    else:
        fv = fs.get_feature_view(job_conf["name"], version=job_conf["version"])
        entity = fv._feature_view_engine._get_training_dataset_metadata(
            feature_view_obj=fv,
            training_dataset_version=job_conf["td_version"],
        )

    entity.compute_statistics()


def ge_validate(job_conf: Dict[Any, Any]) -> None:
    """
    Run expectation suite attached to a feature group.
    """
    feature_store = job_conf.pop("feature_store")
    fs = get_feature_store_handle(feature_store)

    entity = fs.get_feature_group(name=job_conf["name"], version=job_conf["version"])

    # when user runs job we always want to save the report and actually perform validation,
    # no matter of setting on feature group level
    entity.validate(
        dataframe=None, save_report=True, validation_options={"run_validation": True}
    )


def import_fg(job_conf: Dict[Any, Any]) -> None:
    """
    Import data to a feature group using storage connector.
    """
    feature_store = job_conf.pop("feature_store")
    fs = get_feature_store_handle(feature_store)
    # retrieve connector
    st = fs.get_storage_connector(name=job_conf["storageConnectorName"])
    # first read data from connector
    spark_options = job_conf.pop("options")
    df = st.read(query=(job_conf.pop("query", "") or ""), options=spark_options)
    # store dataframe into feature group
    if job_conf["statisticsConfig"]:
        stat_config = StatisticsConfig.from_response_json(job_conf["statisticsConfig"])
    else:
        stat_config = None
    # create fg and insert
    fg = fs.get_or_create_feature_group(
        name=job_conf["featureGroupName"],
        version=job_conf["version"],
        primary_key=job_conf["primaryKey"],
        online_enabled=job_conf.pop("onlineEnabled", False) or False,
        statistics_config=stat_config,
        partition_key=job_conf.pop("partitionKey", []) or [],
        description=job_conf["description"],
        event_time=job_conf.pop("eventTime", None) or None,
    )
    fg.insert(df)


def run_feature_monitoring(job_conf: Dict[str, str]) -> None:
    """
    Run feature monitoring for a given entity (feature_group or feature_view)
    based on a feature monitoring configuration.
    """
    feature_store = job_conf.pop("feature_store")
    fs = get_feature_store_handle(feature_store)

    if job_conf["entity_type"].upper() == "FEATUREGROUPS":
        entity = fs.get_feature_group(
            name=job_conf["name"], version=job_conf["version"]
        )
        feature_group_id = entity._id
        feature_view_name, feature_view_version = None, None
    else:
        feature_group_id = None
        entity = fs.get_feature_view(name=job_conf["name"], version=job_conf["version"])
        feature_view_name, feature_view_version = (
            entity.name,
            entity.version,
        )

    monitoring_config_engine = (
        feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=fs._id,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
        )
    )

    try:
        monitoring_config_engine.run_feature_monitoring(
            entity=entity,
            config_name=job_conf["config_name"],
        )
    except Exception as e:
        config = monitoring_config_engine.get_feature_monitoring_configs(
            name=job_conf["config_name"]
        )
        monitoring_config_engine._result_engine.save_feature_monitoring_result_with_exception(
            config_id=config.id,
            job_name=config.job_name,
            feature_name=config.feature_name,
        )
        raise e


def delta_vacuum_fg(spark: SparkSession, job_conf: Dict[Any, Any]) -> None:
    """
    Run delta vacuum on a feature group.
    """
    feature_store = job_conf.pop("feature_store")
    fs = get_feature_store_handle(feature_store)

    entity = fs.get_feature_group(name=job_conf["name"], version=job_conf["version"])

    entity.delta_vacuum()

def offline_fg_materialization(spark: SparkSession, job_conf: Dict[Any, Any], initial_check_point_string: str) -> None:
    """
    Run materialization job on a feature group.
    """
    feature_store = job_conf.pop("feature_store")
    fs = get_feature_store_handle(feature_store)

    entity = fs.get_feature_group(name=job_conf["name"], version=job_conf["version"])

    read_options = kafka_engine.get_kafka_config(
        entity.feature_store_id, {}, engine="spark"
    )

    # get starting offsets
    offset_location = entity.prepare_spark_location() + "/kafka_offsets"
    try:
        if initial_check_point_string:
            starting_offset_string = json.dumps(_build_offsets(initial_check_point_string))
        else:
            starting_offset_string = spark.read.json(offset_location).toJSON().first()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        # if all else fails read from the beggining
        initial_check_point_string = kafka_engine.kafka_get_offsets(
            topic_name=entity._online_topic_name,
            feature_store_id=entity.feature_store_id,
            offline_write_options={},
            high=False,
        )
        starting_offset_string = json.dumps(_build_offsets(initial_check_point_string))
    print(f"startingOffsets: {starting_offset_string}")

    # get ending offsets
    ending_offset_string = kafka_engine.kafka_get_offsets(
        topic_name=entity._online_topic_name,
        feature_store_id=entity.feature_store_id,
        offline_write_options={},
        high=True,
    )
    ending_offset_string = json.dumps(_build_offsets(ending_offset_string))
    print(f"endingOffsets: {ending_offset_string}")

    # read kafka topic
    df = (
        spark.read.format("kafka")
        .options(**read_options)
        .option("subscribe", entity._online_topic_name)
        .option("startingOffsets", starting_offset_string)
        .option("endingOffsets", ending_offset_string)
        .option("includeHeaders", "true")
        .option("failOnDataLoss", "false")
        .load()
    )

    # filter only the necassary entries
    filtered_df = df.filter(expr("CAST(filter(headers, header -> header.key = 'featureGroupId')[0].value AS STRING)") == str(entity._id))
    filtered_df = filtered_df.filter(expr("CAST(filter(headers, header -> header.key = 'subjectId')[0].value AS STRING)") == str(entity.subject["id"]))

    # limit the number of records ingested
    limit = job_conf.get("write_options", {}).get("job_limit", 5000000)
    filtered_df = filtered_df.limit(limit)

    # deserialize dataframe so that it can be properly saved
    deserialized_df = engine.get_instance()._deserialize_from_avro(entity, filtered_df)

    # insert data
    entity.stream = False # to make sure we dont write to kafka
    entity.insert(deserialized_df, storage="offline")

    # update offsets
    df_offsets = (df if limit > filtered_df.count() else filtered_df).groupBy('partition').agg(max('offset').alias('offset')).collect()
    offset_dict = json.loads(starting_offset_string)
    for offset_row in df_offsets:
        offset_dict[f"{entity._online_topic_name}"][f"{offset_row.partition}"] = offset_row.offset + 1

    # save offsets
    offset_df = spark.createDataFrame([offset_dict])
    offset_df.coalesce(1).write.mode("overwrite").json(offset_location)

def update_table_schema_fg(spark: SparkSession, job_conf: Dict[Any, Any]) -> None:
    """
    Run table schema update job on a feature group.
    """
    feature_store = job_conf.pop("feature_store")
    fs = get_feature_store_handle(feature_store)

    entity = fs.get_feature_group(name=job_conf["name"], version=job_conf["version"])

    entity.stream = False
    engine.get_instance().update_table_schema(entity)

def _build_offsets(initial_check_point_string: str):
    if not initial_check_point_string:
        return ""

    # Split the input string into the topic and partition-offset pairs
    topic, offsets = initial_check_point_string.split(',', 1)
    
    # Split the offsets and build a dictionary from them
    offsets_dict = {partition: int(offset) for partition, offset in (pair.split(':') for pair in offsets.split(','))}
    
    # Create the final dictionary structure
    result = {topic: offsets_dict}
    
    return result

if __name__ == "__main__":
    # Setup spark first so it fails faster in case of args errors
    # Otherwise the resource manager will wait until the spark application master
    # registers, which never happens.
    spark = setup_spark()

    parser = argparse.ArgumentParser(description="HSFS Job Utils")
    parser.add_argument(
        "-op",
        type=str,
        choices=[
            "insert_fg",
            "create_td",
            "create_fv_td",
            "compute_stats",
            "ge_validate",
            "import_fg",
            "run_feature_monitoring",
            "delta_vacuum_fg",
            "offline_fg_materialization",
            "update_table_schema_fg",
        ],
        help="Operation type",
    )
    parser.add_argument(
        "-path",
        type=str,
        help="Location on HopsFS of the JSON containing the full configuration",
    )

    def parse_isoformat_date(da: str) -> datetime:
        # 'Z' is supported in Python 3.11+ so we need to replace it in 3.10
        return datetime.fromisoformat(da.replace("Z", "+00:00"))

    parser.add_argument(
        "-start_time",
        type=parse_isoformat_date,
        help="Job start time",
    )

    parser.add_argument(
        "-initialCheckPointString",
        type=str,
        help="Kafka offset to start consuming from",
    )

    args = parser.parse_args()
    job_conf = read_job_conf(args.path)

    success = False
    try:
        if args.op == "insert_fg":
            insert_fg(spark, job_conf)
        elif args.op == "create_td":
            create_td(job_conf)
        elif args.op == "create_fv_td":
            create_fv_td(job_conf)
        elif args.op == "compute_stats":
            compute_stats(job_conf)
        elif args.op == "ge_validate":
            ge_validate(job_conf)
        elif args.op == "import_fg":
            import_fg(job_conf)
        elif args.op == "run_feature_monitoring":
            run_feature_monitoring(job_conf)
        elif args.op == "delta_vacuum_fg":
            delta_vacuum_fg(spark, job_conf)
        elif args.op == "offline_fg_materialization":
            offline_fg_materialization(spark, job_conf, args.initialCheckPointString)
        elif args.op == "update_table_schema_fg":
            update_table_schema_fg(spark, job_conf)

        success = True
    except Exception:
        # Printing stack trace of exception so that logs are not lost.
        print(traceback.format_exc())
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as e:
                print(f"Error stopping spark session: {e}")
        if not success:
            sys.exit(1)

    sys.exit(0)
