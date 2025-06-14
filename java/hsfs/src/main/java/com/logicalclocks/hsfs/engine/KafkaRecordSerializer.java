/*
 *  Copyright (c) 2025. Hopsworks AB
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

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaRecordSerializer {

  private final String topic;
  private final List<String> primaryKeys;
  private final Map<String, byte[]> headerMap;

  KafkaRecordSerializer(StreamFeatureGroup streamFeatureGroup,  Long numEntries)
          throws FeatureStoreException, IOException {
    this.topic = streamFeatureGroup.getOnlineTopicName();
    this.primaryKeys = streamFeatureGroup.getPrimaryKeys();
    this.headerMap = FeatureGroupUtils.getHeaders(streamFeatureGroup, numEntries);
  }

  public ProducerRecord<byte[], byte[]> serialize(GenericRecord genericRecord) {
    byte[] key = this.serializeKey(genericRecord);
    byte[] value = this.serializeValue(genericRecord);
    ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic, null, key, value);
    for (Map.Entry<String, byte[]> entry: headerMap.entrySet()) {
      producerRecord.headers().add(entry.getKey(), entry.getValue());
    }
    return producerRecord;
  }

  public byte[] serializeKey(GenericRecord genericRecord) {
    List<String> primaryKeyValues = new ArrayList<>();
    for (String primaryKey: primaryKeys) {
      primaryKeyValues.add(genericRecord.get(primaryKey).toString());
    }
    return String.join(";", primaryKeyValues).getBytes(StandardCharsets.UTF_8);
  }

  public byte[] serializeValue(GenericRecord genericRecord) {
    DatumWriter<GenericRecord> datumWriter = new ReflectDatumWriter<>(genericRecord.getSchema());
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    byteArrayOutputStream.reset();

    BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
    try {
      datumWriter.write(genericRecord, binaryEncoder);
      binaryEncoder.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return byteArrayOutputStream.toByteArray();
  }
}