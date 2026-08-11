/*
 *  Copyright (c) 2026-2026. Hopsworks AB
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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class TestDeltaStreamerAvroDeserializer {

  private static final String TOPIC = "test_topic";
  private static final String SUBJECT = "7";
  private static final String FEATURE_GROUP = "10";
  private static final String SCHEMA_STRING =
      "{\"type\":\"record\",\"name\":\"test_fg\",\"namespace\":\"test_featurestore.db\",\"fields\":"
      + "[{\"name\":\"id\",\"type\":[\"null\",\"long\"]}]}";

  private DeltaStreamerAvroDeserializer deserializer() {
    Map<String, String> configs = new HashMap<>();
    configs.put(HudiEngine.SUBJECT_ID, SUBJECT);
    configs.put(HudiEngine.FEATURE_GROUP_ID, FEATURE_GROUP);
    configs.put(HudiEngine.FEATURE_GROUP_SCHEMA, SCHEMA_STRING);
    configs.put(HudiEngine.FEATURE_GROUP_ENCODED_SCHEMA, SCHEMA_STRING);
    configs.put(HudiEngine.FEATURE_GROUP_COMPLEX_FEATURES, "[]");

    DeltaStreamerAvroDeserializer deserializer = new DeltaStreamerAvroDeserializer();
    deserializer.configure(configs, false);
    return deserializer;
  }

  private byte[] encodedRow(long id) throws IOException {
    Schema schema = new Schema.Parser().parse(SCHEMA_STRING);
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", id);

    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(record, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  private Headers headers(String subjectId, String featureGroupId, String operation) {
    RecordHeaders headers = new RecordHeaders();
    headers.add("subjectId", subjectId.getBytes(StandardCharsets.UTF_8));
    headers.add("featureGroupId", featureGroupId.getBytes(StandardCharsets.UTF_8));
    if (operation != null) {
      headers.add("operation", operation.getBytes(StandardCharsets.UTF_8));
    }
    return headers;
  }

  @Test
  void testDeleteTombstoneIsDropped() throws IOException {
    Headers headers = headers(SUBJECT, FEATURE_GROUP, "delete");

    Assertions.assertNull(deserializer().deserialize(TOPIC, headers, encodedRow(2L)));
  }

  @Test
  void testOperationHeaderMatchesDeleteExactly() throws IOException {
    for (String operation : new String[] {"Delete", "DELETE", " delete", "deleted", "upsert"}) {
      Headers headers = headers(SUBJECT, FEATURE_GROUP, operation);

      GenericRecord record = deserializer().deserialize(TOPIC, headers, encodedRow(2L));

      Assertions.assertNotNull(record, "operation header " + operation + " must not delete");
      Assertions.assertEquals(2L, record.get("id"));
    }
  }

  @Test
  void testMissingOperationHeaderIsAnUpsert() throws IOException {
    Headers headers = headers(SUBJECT, FEATURE_GROUP, null);

    GenericRecord record = deserializer().deserialize(TOPIC, headers, encodedRow(2L));

    Assertions.assertNotNull(record);
    Assertions.assertEquals(2L, record.get("id"));
  }

  @Test
  void testOtherFeatureGroupIsDropped() throws IOException {
    Headers otherSubject = headers("8", FEATURE_GROUP, null);
    Headers otherFeatureGroup = headers(SUBJECT, "11", null);

    Assertions.assertNull(deserializer().deserialize(TOPIC, otherSubject, encodedRow(2L)));
    Assertions.assertNull(deserializer().deserialize(TOPIC, otherFeatureGroup, encodedRow(2L)));
  }
}
