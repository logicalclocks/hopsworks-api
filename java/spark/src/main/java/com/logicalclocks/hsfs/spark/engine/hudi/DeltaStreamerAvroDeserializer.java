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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.engine.FeatureGroupUtils;
import lombok.SneakyThrows;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeltaStreamerAvroDeserializer implements Deserializer<GenericRecord> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeltaStreamerAvroDeserializer.class);

  private final ObjectMapper objectMapper = new ObjectMapper();
  private String subjectId;
  private String featureGroupId;
  private Schema schema;
  private Schema encodedSchema;
  private final BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(new byte[0], null);
  private List<String> complexFeatures = null;
  private DatumReader<GenericRecord> encodedDatumReader;
  private final FeatureGroupUtils featureGroupUtils = new FeatureGroupUtils();

  private final Map<String, Schema> complexFeatureSchemas = new HashMap<>();
  private final Map<String, DatumReader<GenericRecord>> complexFeaturesDatumReaders = new HashMap<>();

  public DeltaStreamerAvroDeserializer() {
  }

  @SneakyThrows
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.subjectId = (String) configs.get(HudiEngine.SUBJECT_ID);
    this.featureGroupId = (String) configs.get(HudiEngine.FEATURE_GROUP_ID);
    GenericData.get().addLogicalTypeConversion(new Conversions.DecimalConversion());
    String featureGroupSchema = (String) configs.get(HudiEngine.FEATURE_GROUP_SCHEMA);
    String encodedFeatureGroupSchema = configs.get(HudiEngine.FEATURE_GROUP_ENCODED_SCHEMA).toString()
        .replace("\"type\":[\"bytes\",\"null\"]", "\"type\":[\"null\",\"bytes\"]");
    String complexFeatureString = (String) configs.get(HudiEngine.FEATURE_GROUP_COMPLEX_FEATURES);

    try {
      String[] stringArray = objectMapper.readValue(complexFeatureString, String[].class);
      this.complexFeatures = Arrays.asList(stringArray);
    } catch (JsonProcessingException e) {
      throw new SerializationException("Could not deserialize complex feature array: " + complexFeatureString, e);
    }
    // full schema is only to get partial schema of complex features
    this.schema = new Schema.Parser().parse(featureGroupSchema);
    // encoded schema has binary type for complex features
    this.encodedSchema = new Schema.Parser().parse(encodedFeatureGroupSchema);
    this.encodedDatumReader = new GenericDatumReader<>(this.encodedSchema);

    for (String complexFeature : complexFeatures) {
      Schema featureSchema = null;
      try {
        featureSchema = new Schema.Parser().parse(featureGroupUtils.getFeatureAvroSchema(complexFeature, schema));
      } catch (FeatureStoreException | IOException e) {
        throw new SerializationException("Can't deserialize complex feature schema: " + complexFeature, e);
      }

      complexFeatureSchemas.put(complexFeature, featureSchema);
      complexFeaturesDatumReaders.put(complexFeature, new GenericDatumReader<>(featureSchema));
    }
  }

  @Override
  public GenericRecord deserialize(String topic, Headers headers, byte[] data) {
    if (subjectId.equals(getHeader(headers, "subjectId"))
        && featureGroupId.equals(getHeader(headers, "featureGroupId"))) {
      if (skipOfflineMaterialization(headers)) {
        return null;
      }
      return deserialize(topic, data);
    }
    return null; // this job doesn't care about this entry, no point in deserializing
  }

  @Override
  public GenericRecord deserialize(String topic, byte[] data) {
    GenericRecord finalResult = new GenericData.Record(this.schema);
    GenericRecord result = null;

    try {
      if (data != null) {
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, binaryDecoder);
        result = new GenericData.Record(this.encodedSchema);
        result = encodedDatumReader.read(result, decoder);
      }
    } catch (Exception ex) {
      LOGGER.info(
          "Can't deserialize data '" + Arrays.toString(data) + "' from topic '" + topic + "'", ex);
    }

    Schema featureSchema;
    byte[] featureData;
    Decoder decoder;

    for (String complexFeature : complexFeatures) {
      ByteBuffer byteBuffer = (ByteBuffer) result.get(complexFeature);
      if (byteBuffer == null) {
        // This is required since PySpark to_avro encodes 'null' values as null while fast avro encodes it as "\u0000"
        finalResult.put(complexFeature, null);
      } else {
        featureData = new byte[byteBuffer.remaining()];
        byteBuffer.get(featureData);
        featureSchema = complexFeatureSchemas.get(complexFeature);
        try {
          decoder = DecoderFactory.get().binaryDecoder(featureData, binaryDecoder);
          finalResult.put(complexFeature, complexFeaturesDatumReaders.get(complexFeature).read(null, decoder));
        } catch (Exception ex) {
          LOGGER.info(
              "Can't deserialize complex feature data '" + Arrays.toString(featureData) + "' from topic '" + topic
                  + "' with schema: " + featureSchema.toString(true), ex);
        }
      }
    }

    for (String feature : this.schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList())) {
      if (!complexFeatures.contains(feature)) {
        finalResult.put(feature, result.get(feature));
      }
    }
    return finalResult;
  }

  /**
   * Whether this record belongs in the offline table at all.
   *
   * <p>The storage header names the consumer the record is meant for.
   * "online" is for OnlineFS alone, because whoever produced it already wrote the offline leg
   * itself (or has none), so materializing it would write the same rows to the table a second
   * time.
   * Anything else belongs here: the header is absent when both consumers read the record,
   * "offline" when this job is the only one that should, and "1"/"0" from clients that predate
   * this contract meant "ingest online"/"skip online" without ever excluding the offline table.
   *
   * <p>A delete tombstone does not belong here either.
   * It is on the topic for OnlineFS, while removeRows has already applied the offline delete
   * directly to the table, so materializing the tombstone would re-insert the key it deleted.
   * New clients mark it "online", so the operation header covers the ones that do not.
   * When the offline delete moves onto the topic, this becomes a branch that emits a Hudi delete
   * rather than dropping the record.
   *
   * <p>Exact match on both values, as in OnlineFsHandler.getRow.
   */
  private boolean skipOfflineMaterialization(Headers headers) {
    return "online".equals(getHeader(headers, "storage"))
        || "delete".equals(getHeader(headers, "operation"));
  }

  private static String getHeader(Headers headers, String headerKey) {
    Header header = headers.lastHeader(headerKey);
    if (header != null) {
      return new String(header.value(), StandardCharsets.UTF_8);
    }
    return null;
  }

  @Override
  public void close() {

  }
}
