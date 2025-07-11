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

import com.google.common.base.Strings;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.metadata.DatasetApi;
import com.logicalclocks.hsfs.metadata.HopsworksInternalClient;

import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Engine<T> extends EngineBase {

  private static Engine INSTANCE = null;

  private FeatureGroupUtils featureGroupUtils = new FeatureGroupUtils();

  public static synchronized Engine getInstance() throws FeatureStoreException {
    if (INSTANCE == null) {
      INSTANCE = new Engine();
    }
    return INSTANCE;
  }

  private Engine() throws FeatureStoreException {
  }

  public List<T> writeStream(
      StreamFeatureGroup streamFeatureGroup,
      List<T> featureData,
      Map<String, String> writeOptions
  ) throws FeatureStoreException, IOException, SchemaValidationException, NoSuchFieldException, IllegalAccessException {

    Map<String, Schema> complexFeatureSchemas = new HashMap<>();
    for (Object featureName: streamFeatureGroup.getComplexFeatures()) {
      complexFeatureSchemas.put(featureName.toString(),
                  new Schema.Parser().parse(streamFeatureGroup.getFeatureAvroSchema(featureName.toString())));
    }
    Schema featureGroupSchema = new Schema.Parser().parse(streamFeatureGroup.getAvroSchema());
    Schema encodedFeatureGroupSchema = new Schema.Parser().parse(streamFeatureGroup.getEncodedAvroSchema());

    Properties kafkaProps = new Properties();
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProps.putAll(getKafkaConfig(streamFeatureGroup, writeOptions));

    KafkaRecordSerializer kafkaRecordSerializer = new KafkaRecordSerializer(streamFeatureGroup,
            (long) featureData.size());

    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(kafkaProps)) {
      for (Object input : featureData) {
        GenericRecord genericRecord =
            convertPojoToGenericRecord(input, featureGroupSchema, encodedFeatureGroupSchema, complexFeatureSchemas);
        ProducerRecord<byte[], byte[]> record = kafkaRecordSerializer.serialize(genericRecord);

        producer.send(record);
      }
      producer.flush();
    }
    return featureData;
  }

  private GenericRecord convertPojoToGenericRecord(Object input,
                                                   Schema featureGroupSchema,
                                                   Schema encodedFeatureGroupSchema,
                                                   Map<String, Schema> complexFeatureSchemas)
          throws NoSuchFieldException, IllegalAccessException, FeatureStoreException, IOException {

    // Generate the genericRecord without nested serialization.
    // Users have the option of providing directly a GenericRecord.
    // If that's the case we also expect nested structures to be generic records.
    GenericRecord plainRecord;
    if (input instanceof GenericRecord) {
      plainRecord = (GenericRecord) input;
    } else {
      plainRecord = convertPojoToGenericRecord(input, featureGroupSchema);
    }

    // Apply nested serialization for complex features
    GenericRecord encodedRecord = new GenericData.Record(encodedFeatureGroupSchema);
    for (Schema.Field field: encodedFeatureGroupSchema.getFields()) {
      if (complexFeatureSchemas.containsKey(field.name())) {
        Schema complexFieldSchema = complexFeatureSchemas.get(field.name());
        GenericDatumWriter<Object> complexFeatureDatumWriter = new GenericDatumWriter<>(complexFieldSchema);

        try (ByteArrayOutputStream complexFeatureByteArrayOutputStream = new ByteArrayOutputStream()) {
          BinaryEncoder complexFeatureBinaryEncoder =
                  new EncoderFactory().binaryEncoder(complexFeatureByteArrayOutputStream, null);
          complexFeatureDatumWriter.write(plainRecord.get(field.name()), complexFeatureBinaryEncoder);
          complexFeatureBinaryEncoder.flush();

          // Replace the field in the generic record with the serialized version
          encodedRecord.put(field.name(), ByteBuffer.wrap(complexFeatureByteArrayOutputStream.toByteArray()));
        }
      } else {
        encodedRecord.put(field.name(), plainRecord.get(field.name()));
      }
    }

    return encodedRecord;
  }

  private GenericRecord convertPojoToGenericRecord(Object input, Schema featureGroupSchema)
          throws NoSuchFieldException, IllegalAccessException, FeatureStoreException {

    // Create a new Avro record based on the given schema
    GenericRecord record = new GenericData.Record(featureGroupSchema);

    for (Schema.Field schemaField : featureGroupSchema.getFields()) {
      Field pojoField = input.getClass().getDeclaredField(schemaField.name());
      pojoField.setAccessible(true);
      Object pojoValue = pojoField.get(input);
      record.put(schemaField.name(), convertValue(pojoValue, schemaField.schema()));
    }

    return record;
  }


  private Object convertValue(Object value, Schema schema)
      throws NoSuchFieldException, IllegalAccessException, FeatureStoreException {
    if (value == null) {
      return null;
    }

    switch (schema.getType()) {
      case RECORD:
        return convertPojoToGenericRecord(value, schema); // Recursive conversion

      case ARRAY:
        Schema elementType = schema.getElementType();
        if (value instanceof Collection) {
          Collection<?> collection = (Collection<?>) value;
          List<Object> avroList = new ArrayList<>();
          for (Object item : collection) {
            avroList.add(convertValue(item, elementType));
          }
          return avroList;
        } else if (value.getClass().isArray()) {
          List<Object> avroList = new ArrayList<>();
          for (Object item : (Object[]) value) {
            avroList.add(convertValue(item, elementType));
          }
          return avroList;
        }
        throw new FeatureStoreException("Unsupported array type: " + value.getClass());

      case UNION:
        // Unions are tricky: Avro allows [null, "type"]
        for (Schema subSchema : schema.getTypes()) {
          if (subSchema.getType() == Schema.Type.NULL) {
            continue; // Skip null type
          }
          try {
            return convertValue(value, subSchema);
          } catch (Exception ignored) {
            // Try next type in union
          }
        }
        throw new FeatureStoreException("Cannot match union type for value: " + value.getClass());

      case ENUM:
        return new GenericData.EnumSymbol(schema, value.toString());

      case STRING:
        return value.toString();

      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        return value; // Primitive types are directly compatible

      case MAP:
        if (value instanceof Map) {
          Map<String, Object> avroMap = new HashMap<>();
          for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            if (!(entry.getKey() instanceof String)) {
              throw new FeatureStoreException("Avro only supports string keys in maps.");
            }
            avroMap.put(entry.getKey().toString(), convertValue(entry.getValue(), schema.getValueType()));
          }
          return avroMap;
        }
        throw new FeatureStoreException("Unsupported map type: " + value.getClass());

      default:
        throw new FeatureStoreException("Unsupported Avro type: " + schema.getType());
    }
  }

  @Override
  public String addFile(String filePath) throws IOException, FeatureStoreException {
    if (Strings.isNullOrEmpty(filePath)) {
      return filePath;
    }

    String targetPath = System.getProperty("java.io.tmpdir") + filePath.substring(filePath.lastIndexOf("/"));
    try (FileOutputStream outputStream = new FileOutputStream(targetPath)) {
      outputStream.write(DatasetApi.readContent(filePath, featureGroupUtils.getDatasetType(filePath)));
    }
    return targetPath;
  }

  @Override
  public Map<String, String> getKafkaConfig(FeatureGroupBase featureGroup, Map<String, String> writeOptions)
            throws FeatureStoreException, IOException {
    boolean external = !(System.getProperties().containsKey(HopsworksInternalClient.REST_ENDPOINT_SYS)
                || (writeOptions != null
                && Boolean.parseBoolean(writeOptions.getOrDefault("internal_kafka", "false"))));

    StorageConnector.KafkaConnector storageConnector =
                storageConnectorApi.getKafkaStorageConnector(featureGroup.getFeatureStore(), external);
    storageConnector.setSslTruststoreLocation(addFile(storageConnector.getSslTruststoreLocation()));
    storageConnector.setSslKeystoreLocation(addFile(storageConnector.getSslKeystoreLocation()));

    Map<String, String> config = storageConnector.kafkaOptions();

    if (writeOptions != null) {
      config.putAll(writeOptions);
    }
    config.put("enable.idempotence", "false");
    return config;
  }
}
