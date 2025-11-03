/*
 *  Copyright (c) 2021-2023. Hopsworks AB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.logicalclocks.hsfs.spark.engine.hudi;

import org.apache.avro.generic.GenericRecord;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.config.KafkaSourceConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.sources.KafkaSource;
import org.apache.hudi.utilities.sources.helpers.AvroConvertor;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;

import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.StreamContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;

public class DeltaStreamerKafkaSource extends KafkaSource<JavaRDD<GenericRecord>> {
  private static final Logger LOG = LogManager.getLogger(DeltaStreamerKafkaSource.class);
  private static final String NATIVE_KAFKA_KEY_DESERIALIZER_PROP = "key.deserializer";
  private static final String NATIVE_KAFKA_VALUE_DESERIALIZER_PROP = "value.deserializer";

  public DeltaStreamerKafkaSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider, HoodieIngestionMetrics metrics) {
    this(props, sparkContext, sparkSession, metrics, new DefaultStreamContext(schemaProvider, Option.empty()));
  }
  
  public DeltaStreamerKafkaSource(TypedProperties properties, JavaSparkContext sparkContext, SparkSession sparkSession,
      HoodieIngestionMetrics metrics, StreamContext streamContext) {
    super(properties, sparkContext, sparkSession, SourceType.AVRO, metrics,
        new DefaultStreamContext(UtilHelpers.getSchemaProviderForKafkaSource(streamContext.getSchemaProvider(),
            properties, sparkContext), streamContext.getSourceProfileSupplier()));
    props.put(NATIVE_KAFKA_KEY_DESERIALIZER_PROP, StringDeserializer.class.getName());
    
    String deserializerClassName =
        props.getString(DataSourceWriteOptions.KAFKA_AVRO_VALUE_DESERIALIZER_CLASS().key(), "");
    if (deserializerClassName.isEmpty()) {
      props.put(NATIVE_KAFKA_VALUE_DESERIALIZER_PROP, DeltaStreamerAvroDeserializer.class.getName());
    } else {
      try {
        if (schemaProvider == null) {
          throw new HoodieIOException("SchemaProvider has to be set to use custom Deserializer");
        }
        props.put(DataSourceWriteOptions.SCHEMA_PROVIDER_CLASS_PROP(), schemaProvider.getClass().getName());
        props.put(NATIVE_KAFKA_VALUE_DESERIALIZER_PROP, Class.forName(deserializerClassName));
      } catch (ClassNotFoundException var9) {
        String error = "Could not load custom avro kafka deserializer: " + deserializerClassName;
        LOG.error(error);
        throw new HoodieException(error, var9);
      }
    }
    offsetGen = new KafkaOffsetGen(props);
  }
  
  
  @Override
  protected InputBatch<JavaRDD<GenericRecord>> readFromCheckpoint(Option<Checkpoint> lastCheckpointStr,
      long sourceLimit) {
    return super.readFromCheckpoint(lastCheckpointStr, sourceLimit);
  }
  
  protected JavaRDD<GenericRecord> maybeAppendKafkaOffsets(JavaRDD<ConsumerRecord<Object, Object>> kafkaRDd) {
    if (shouldAddOffsets) {
      AvroConvertor convertor = new AvroConvertor(schemaProvider.getSourceSchema());
      return kafkaRDd.map(convertor::withKafkaFieldsAppended);
    } else {
      return kafkaRDd.map(consumerRecord -> (GenericRecord) consumerRecord.value());
    }
  }
  
  @Override
  protected JavaRDD<GenericRecord> toBatch(OffsetRange[] offsetRanges) {
    return maybeAppendKafkaOffsets(KafkaUtils.createRDD(sparkContext, offsetGen.getKafkaParams(), offsetRanges,
        LocationStrategies.PreferConsistent()).filter(consemerRec -> consemerRec.value() != null));
  }

  @Override
  public void onCommit(String lastCkptStr) {
    if (getBooleanWithAltKeys(props, KafkaSourceConfig.ENABLE_KAFKA_COMMIT_OFFSET)) {
      LOG.info("Committing offset: " + lastCkptStr);
      offsetGen.commitOffsetToKafka(lastCkptStr);
    }
  }
}
