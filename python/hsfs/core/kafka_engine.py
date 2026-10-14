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

import contextlib
import json
import time
import warnings
from datetime import datetime, timezone
from io import BytesIO
from typing import TYPE_CHECKING, Any, Literal

from hopsworks_common import client
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.core.constants import (
    HAS_AVRO,
    HAS_CONFLUENT_KAFKA,
    HAS_FAST_AVRO,
    HAS_NUMPY,
    HAS_PANDAS,
    avro_not_installed_message,
)
from hopsworks_common.decorators import _uses_confluent_kafka
from hsfs.core import online_ingestion, online_ingestion_api, storage_connector_api
from tqdm import tqdm


if HAS_NUMPY:
    import numpy as np

if HAS_PANDAS:
    import pandas as pd

if HAS_CONFLUENT_KAFKA:
    from confluent_kafka import (
        Consumer,
        KafkaError,
        KafkaException,
        Producer,
        TopicPartition,
    )

if HAS_FAST_AVRO:
    from fastavro import schemaless_writer
    from fastavro.schema import parse_schema
elif HAS_AVRO:
    import avro.io
    import avro.schema


if TYPE_CHECKING:
    from collections.abc import Callable

    from hsfs.feature_group import ExternalFeatureGroup, FeatureGroup


# region Storage header
# The `storage` header names which of the two consumers of the online topic is meant to
# ingest a record, mirroring the `storage` argument of
# [`FeatureGroup.insert`][hsfs.feature_group.FeatureGroup.insert]:
#
#   absent      both OnlineFS and the offline materialization job ingest the record
#   b"online"   OnlineFS only; the materialization job skips the record
#   b"offline"  the materialization job only; OnlineFS skips the record
#
# Older clients wrote a per-row flag into the same header instead: b"0" meant "skip
# online" (equivalent to b"offline") and b"1" meant "ingest online" while the record was
# still materialized offline (equivalent to absent).
# Both consumers keep reading b"0" as b"offline"; the producers here no longer emit either
# value.
# Only set the header when the record has a single destination: leaving it out is both the
# default and the cheapest option on the wire, which matters per record at ingestion scale.
_STORAGE_ONLINE = "online"
_STORAGE_OFFLINE = "offline"
# endregion


@_uses_confluent_kafka
def _init_kafka_consumer(
    feature_store_id: int,
    offline_write_options: dict[str, Any],
) -> Consumer:
    # setup kafka consumer
    consumer_config = _get_kafka_config(feature_store_id, offline_write_options)
    if "group.id" not in consumer_config:
        consumer_config["group.id"] = "hsfs_consumer_group"

    return Consumer(consumer_config)


def _get_kafka_resources(
    feature_group: FeatureGroup | ExternalFeatureGroup,
    offline_write_options: dict[str, Any],
    num_entries: int | None = None,
    storage: str | None = None,
) -> tuple[
    Producer, dict[str, bytes], dict[str, Callable[..., bytes]], Callable[..., bytes] :
]:
    # this function is a caching wrapper around _init_kafka_resources
    if feature_group._multi_part_insert and feature_group._kafka_producer:
        return (
            feature_group._kafka_producer,
            feature_group._kafka_headers,
            feature_group._feature_writers,
            feature_group._writer,
        )
    producer, headers, feature_writers, writer = _init_kafka_resources(
        feature_group, offline_write_options, num_entries, storage
    )
    if feature_group._multi_part_insert:
        feature_group._kafka_producer = producer
        feature_group._kafka_headers = headers
        feature_group._feature_writers = feature_writers
        feature_group._writer = writer
    return producer, headers, feature_writers, writer


def _init_kafka_resources(
    feature_group: FeatureGroup | ExternalFeatureGroup,
    offline_write_options: dict[str, Any],
    num_entries: int | None = None,
    storage: str | None = None,
) -> tuple[
    Producer, dict[str, bytes], dict[str, Callable[..., bytes]], Callable[..., bytes] :
]:
    # setup kafka producer
    producer = _init_kafka_producer(
        feature_group.feature_store_id, offline_write_options
    )
    # setup headers
    headers = _get_headers(
        feature_group, num_entries, offline_write_options, storage=storage
    )
    # setup writers
    feature_writers, writer = _get_writer_function(feature_group)

    return producer, headers, feature_writers, writer


def _get_writer_function(
    feature_group: FeatureGroup | ExternalFeatureGroup,
) -> tuple[dict[str, Callable[..., bytes]], Callable[..., bytes]]:
    # setup complex feature writers
    feature_writers = {
        feature: _get_encoder_func(feature_group._get_feature_avro_schema(feature))
        for feature in feature_group.get_complex_features()
    }
    # setup row writer function
    writer = _get_encoder_func(feature_group._get_encoded_avro_schema())
    return (feature_writers, writer)


def _online_delete_fill_values(
    feature_group: FeatureGroup | ExternalFeatureGroup,
) -> dict[str, Any]:
    """Null fill values for every non-primary-key field in the feature group schema.

    An online delete tombstone is serialized against the full feature group Avro
    schema, but the caller only needs to pass the primary key (the offline delete
    contract).
    Non-key fields are filled with null so the record serializes; OnlineFS deletes
    by primary key and discards the values.
    Online feature group schemas make every non-key field a nullable union, so null
    always serializes.
    """
    primary_key = set(feature_group.primary_key)
    fields = json.loads(feature_group.avro_schema)["fields"]
    return {field["name"]: None for field in fields if field["name"] not in primary_key}


def _get_headers(
    feature_group: FeatureGroup | ExternalFeatureGroup,
    num_entries: int | None = None,
    options: dict[str, Any] | None = None,
    operation: str | None = None,
    storage: str | None = None,
) -> dict[str, bytes]:
    """Kafka headers for the records of one write to the online topic.

    `storage` is the destination of this write: `"online"` for records only OnlineFS
    should ingest, `"offline"` for records only the offline materialization job should
    ingest, `None` when both consume them.
    See the storage header contract at the top of this module.
    """
    # custom headers for hopsworks onlineFS
    headers = {
        "projectId": str(feature_group.feature_store.project_id).encode("utf8"),
        "featureGroupId": str(feature_group._id).encode("utf8"),
        "subjectId": str(feature_group.subject["id"]).encode("utf8"),
    }

    # operation header tells OnlineFS whether the message is an upsert (absent) or a
    # delete tombstone ("delete"); OnlineFS deletes the row by primary key on "delete".
    if operation is not None:
        headers["operation"] = operation.encode("utf8")

    if storage is not None:
        headers["storage"] = storage.encode("utf8")

    online_ingestion_options = (
        options.get("online_ingestion_options") if options else None
    )
    if online_ingestion_options and online_ingestion_options.get("upsert_if_newer"):
        headers["upsertIfNewer"] = b"1"

    # An offline-only write reaches no online store, so it gets no online ingestion to
    # report progress against: creating one would leave it forever short of its entries.
    if feature_group.online_enabled and storage != _STORAGE_OFFLINE:
        # setup online ingestion id
        online_ingestion_instance = (
            online_ingestion_api.OnlineIngestionApi()._create_online_ingestion(
                feature_group, online_ingestion.OnlineIngestion(num_entries=num_entries)
            )
        )
        headers["onlineIngestionId"] = str(online_ingestion_instance.id).encode("utf8")

    return headers


def _wait_for_online_ingestion(
    feature_group: FeatureGroup | ExternalFeatureGroup,
    headers: dict[str, bytes],
    options: dict[str, Any],
) -> None:
    """Block until OnlineFS has ingested the records written with `headers`, if `options` asks to wait.

    Returns at once for records that carry no online ingestion id, such as those of an offline-only write.
    Warns and returns when the ingestion no longer exists, as the backend prunes the oldest ingestions of a feature group.
    """
    if not options.get("wait_for_online_ingestion", False):
        return
    online_ingestion_id = headers.get("onlineIngestionId")
    if online_ingestion_id is None:
        return
    online_ingestion_id = int(online_ingestion_id)
    # Not get_latest_online_ingestion: the backend picks the latest by id, and NDB hands out
    # auto-increment ids in per-mysqld blocks, so with several mysqlds the latest by id can be
    # an older ingestion that has already completed.
    online_ingestion_instance = feature_group.get_online_ingestion(online_ingestion_id)
    if online_ingestion_instance is None:
        warnings.warn(
            f"Online ingestion {online_ingestion_id} of feature group '{feature_group.name}' "
            "was pruned by the backend before the write could wait for it, so the write returns "
            "without knowing whether its rows have reached the online feature store.",
            stacklevel=1,
        )
        return
    online_ingestion_instance.wait_for_completion(
        options=options.get("online_ingestion_options", {})
    )


@_uses_confluent_kafka
def _init_kafka_producer(
    feature_store_id: int,
    offline_write_options: dict[str, Any],
) -> Producer:
    # setup kafka producer
    return Producer(_get_kafka_config(feature_store_id, offline_write_options))


def _get_watermark_offsets(
    consumer: Consumer, partition: TopicPartition, timeout: float
) -> tuple[int, int]:
    """Read a partition's watermarks, waiting out a partition whose leader is not serving yet.

    A topic is listed as soon as it is created, but each broker answers for its partitions
    only once it has loaded them, a fraction of a second later; a partition whose leader
    just moved is the same. Until then the broker refuses the read with one of the errors
    below, so the read is retried with a short backoff for up to `timeout` seconds rather
    than failing the insert that asked for the offsets.
    """
    deadline = time.monotonic() + timeout
    delay = 0.1
    while True:
        try:
            return consumer.get_watermark_offsets(partition)
        except KafkaException as e:
            error = e.args[0] if e.args else None
            transient = isinstance(error, KafkaError) and error.code() in (
                KafkaError.NOT_LEADER_FOR_PARTITION,
                KafkaError.LEADER_NOT_AVAILABLE,
                KafkaError.UNKNOWN_TOPIC_OR_PART,
            )
            if not transient or time.monotonic() + delay > deadline:
                raise
        time.sleep(delay)
        delay = min(delay * 2, 1.0)


@_uses_confluent_kafka
def _kafka_get_offsets(
    topic_name: str,
    feature_store_id: int,
    offline_write_options: dict[str, Any],
    high: bool,
) -> str:
    consumer = _init_kafka_consumer(feature_store_id, offline_write_options)
    try:
        timeout = offline_write_options.get("kafka_timeout", 6)
        topics = consumer.list_topics(timeout=timeout).topics
        if topic_name not in topics:
            return ""
        offsets = ""
        tuple_value = int(high)
        for partition_metadata in topics.get(topic_name).partitions.values():
            partition = TopicPartition(
                topic=topic_name, partition=partition_metadata.id
            )
            watermarks = _get_watermark_offsets(consumer, partition, timeout)
            offsets += f",{partition_metadata.id}:{watermarks[tuple_value]}"
        return f"{topic_name + offsets}"
    finally:
        consumer.close()


@_uses_confluent_kafka
def _kafka_get_offsets_for_times(
    topic_name: str,
    feature_store_id: int,
    offline_write_options: dict[str, Any],
    timestamp: int,
) -> str:
    """Look up the offsets a topic has to be read from to skip everything older than a timestamp.

    Returns the `topic,partition:offset,...` string
    [`_kafka_get_offsets`][hsfs.core.kafka_engine._kafka_get_offsets] returns, holding for
    each partition the earliest offset whose record is at or after `timestamp`.
    A partition whose records all predate `timestamp` contributes the high watermark it had
    before the lookup, since it held nothing worth reading then; a record appended after the
    lookup lands at or past that watermark and is still read.
    A partition Kafka could not answer for contributes
    its low watermark, so an unanswered lookup reads too much rather than too little.
    The empty string is returned when the topic does not exist.

    Parameters:
        topic_name: Name of the topic to look the offsets up in.
        feature_store_id: Id of the feature store the topic belongs to.
        offline_write_options: Options the consumer is built from, honouring `kafka_timeout`.
        timestamp: Unix timestamp in milliseconds to look the offsets up at.

    Returns:
        The offsets, in the same format as `_kafka_get_offsets`.
    """
    consumer = _init_kafka_consumer(feature_store_id, offline_write_options)
    try:
        timeout = offline_write_options.get("kafka_timeout", 6)
        topics = consumer.list_topics(timeout=timeout).topics
        if topic_name not in topics:
            return ""

        partitions = [
            partition_metadata.id
            for partition_metadata in topics.get(topic_name).partitions.values()
        ]
        # Captured before the lookup: a high watermark read after it could already be past
        # a record appended in between, which the lookup did not see and the read would
        # then skip.
        highs = {
            partition: _get_watermark_offsets(
                consumer, TopicPartition(topic=topic_name, partition=partition), timeout
            )[1]
            for partition in partitions
        }
        lookups = [
            TopicPartition(topic=topic_name, partition=partition, offset=timestamp)
            for partition in partitions
        ]
        offsets = ""
        for result in consumer.offsets_for_times(lookups, timeout=timeout):
            # Read after the lookup, since retention can drop the record any offset below
            # points at.
            low, _ = _get_watermark_offsets(
                consumer,
                TopicPartition(topic=topic_name, partition=result.partition),
                timeout,
            )
            if result.error is not None:
                # Reading from the low watermark is what this function is meant to avoid,
                # but a partition Kafka would not answer for has no offset to trust, and
                # re-reading records is recoverable where skipping them is not.
                offset = low
            elif result.offset < 0:
                # Kafka answers a timestamp past the last record with a negative offset:
                # every record the partition holds is older than the timestamp, so the
                # reader belongs at the end of it.
                offset = max(highs[result.partition], low)
            else:
                offset = max(result.offset, low)
            offsets += f",{result.partition}:{offset}"

        return f"{topic_name + offsets}"
    finally:
        consumer.close()


def _kafka_produce(
    producer: Producer,
    key: str,
    encoded_row: bytes,
    topic_name: str,
    headers: dict[str, bytes],
    acked: callable,
    debug_kafka: bool = False,
) -> None:
    while True:
        # if BufferError is thrown, we can be sure, message hasn't been send so we retry
        try:
            # produce
            producer.produce(
                topic=topic_name,
                key=key,
                value=encoded_row,
                callback=acked,
                headers=headers,
            )

            # Trigger internal callbacks to empty op queue
            producer.poll(0)
            break
        except BufferError as e:
            if debug_kafka:
                print(f"Caught: {e}")
            # backoff for 1 second
            producer.poll(1)


def _encode_complex_features(
    feature_writers: dict[str, callable], row: dict[str, Any]
) -> dict[str, Any]:
    for feature_name, writer in feature_writers.items():
        with BytesIO() as outf:
            writer(row[feature_name], outf)
            row[feature_name] = outf.getvalue()
    return row


def _get_encoder_func(writer_schema: str) -> callable:
    if HAS_FAST_AVRO:
        schema = json.loads(writer_schema)
        parsed_schema = parse_schema(schema)
        return lambda record, outf: schemaless_writer(outf, parsed_schema, record)

    if not HAS_AVRO:
        raise ModuleNotFoundError(avro_not_installed_message)

    parsed_schema = avro.schema.parse(writer_schema)
    writer = avro.io.DatumWriter(parsed_schema)
    return lambda record, outf: writer.write(record, avro.io.BinaryEncoder(outf))


def _encode_row(complex_feature_writers, writer, row):
    # transform special data types
    # here we might need to handle also timestamps and other complex types
    # possible optimizaiton: make it based on type so we don't need to loop over
    # all keys in the row
    if isinstance(row, dict):
        for k in row:
            # NaT is a datetime subclass, so it would pass the timezone branch below and reach avro as a
            # timestamp it cannot write.
            if HAS_PANDAS and row[k] is pd.NaT:
                row[k] = None
                continue
            # for avro to be able to serialize them, they need to be python data types
            if HAS_NUMPY and isinstance(row[k], np.ndarray):
                row[k] = row[k].tolist()
            if HAS_PANDAS and isinstance(row[k], pd.Timestamp):
                row[k] = row[k].to_pydatetime()
            if isinstance(row[k], datetime) and row[k].tzinfo is None:
                row[k] = row[k].replace(tzinfo=timezone.utc)
            if HAS_PANDAS and isinstance(row[k], pd._libs.missing.NAType):
                row[k] = None
    # encode complex features
    row = _encode_complex_features(complex_feature_writers, row)
    # encode feature row
    with BytesIO() as outf:
        writer(row, outf)
        return outf.getvalue()


def _get_kafka_config(
    feature_store_id: int,
    write_options: dict[str, Any] | None = None,
    engine: Literal["spark", "confluent"] = "confluent",
) -> dict[str, Any]:
    if write_options is None:
        write_options = {}
    external = client._is_external() and not write_options.get("internal_kafka", False)

    storage_connector = (
        storage_connector_api.StorageConnectorApi()._get_kafka_connector(
            feature_store_id, external
        )
    )

    if engine == "spark":
        config = storage_connector.spark_options()
        config.update(write_options)
    elif engine == "confluent":
        config = storage_connector.confluent_options()
        config.update(write_options.get("kafka_producer_config", {}))
    return config


@_uses_confluent_kafka
def _build_ack_callback_and_optional_progress_bar(
    n_rows: int, is_multi_part_insert: bool, offline_write_options: dict[str, Any]
) -> tuple[Callable, tqdm | None]:
    if not is_multi_part_insert:
        if n_rows is None:
            bar_format = "{desc}: {n_fmt} Rows | Elapsed Time: {elapsed}"
        else:
            bar_format = (
                "{desc}: {percentage:.2f}% |{bar}| Rows {n_fmt}/{total_fmt} | "
                "Elapsed Time: {elapsed} | Remaining Time: {remaining}"
            )
        progress_bar = tqdm(
            total=n_rows,
            bar_format=bar_format,
            desc="Uploading Dataframe",
            mininterval=1,
        )
    else:
        progress_bar = None

    def acked(err: KafkaError | None, msg: Any) -> None:
        if err is not None:
            if offline_write_options.get("debug_kafka", False):
                print(f"Failed to deliver message: {str(msg)}: {str(err)}")
            if progress_bar is not None:
                progress_bar.colour = "RED"
            raise _delivery_error(err, msg)
        # update progress bar for each msg
        if not is_multi_part_insert:
            progress_bar.update()

    return acked, progress_bar


def _delivery_error(err: KafkaError, msg: Any) -> FeatureStoreException:
    """Build the exception raised when Kafka fails to deliver a produced row.

    Any delivery error means the row never reached the topic, so the online feature store will
    never ingest it.
    Reporting it is what keeps a partially delivered insert from looking like a successful one.

    `confluent_kafka.KafkaError` is an odd type to raise: CPython accepts it as the operand of
    `raise`, yet it does not subclass `BaseException`, so `except Exception` never matches it and
    only an explicit `except KafkaError` would.
    Raising it directly would therefore slip past a caller's error handling, so it is wrapped in a
    `KafkaException` cause, which is an ordinary exception and keeps the original error code
    reachable for callers that need to branch on it.
    """
    location = ""
    # The message handle may be unusable; the error itself is still worth reporting.
    with contextlib.suppress(AttributeError, TypeError):
        location = f" to topic '{msg.topic()}' partition {msg.partition()}"

    hint = ""
    if err.code() in (KafkaError.MSG_SIZE_TOO_LARGE, KafkaError.RECORD_LIST_TOO_LARGE):
        hint = (
            " The row exceeds the maximum message size accepted by the broker."
            " Reduce the size of the row's values, or raise the topic's 'max.message.bytes'"
            " together with the producer's 'message.max.bytes'"
            " (via the 'kafka_producer_config' write option)."
        )

    exception = FeatureStoreException(
        f"Failed to deliver row{location}: {err!s}.{hint}"
        " The insert is incomplete - this row was not written to the online feature store."
    )
    exception.__cause__ = KafkaException(err)
    return exception
