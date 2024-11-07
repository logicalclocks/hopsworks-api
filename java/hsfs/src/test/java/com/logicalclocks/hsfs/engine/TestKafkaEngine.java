/*
 *  Copyright (c) 2024. Hopsworks AB
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

import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.SecurityProtocol;
import com.logicalclocks.hsfs.SslEndpointIdentificationAlgorithm;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.HopsworksHttpClient;
import com.logicalclocks.hsfs.metadata.HopsworksInternalClient;
import com.logicalclocks.hsfs.metadata.Option;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestKafkaEngine {

    @Test
    public void testGetKafkaConfig() throws FeatureStoreException, IOException {
        // Arrange
        HopsworksClient hopsworksClient = Mockito.mock(HopsworksClient.class);
        hopsworksClient.setInstance(new HopsworksClient(Mockito.mock(HopsworksHttpClient.class), "host"));
        System.setProperty(HopsworksInternalClient.REST_ENDPOINT_SYS, "");

        KafkaEngine kafkaEngine = new KafkaEngine(Mockito.mock(EngineBase.class));
        StorageConnectorApi storageConnectorApi = Mockito.mock(StorageConnectorApi.class);
        kafkaEngine.storageConnectorApi = storageConnectorApi;
        StorageConnector.KafkaConnector kafkaConnector = new StorageConnector.KafkaConnector();
        kafkaConnector.setBootstrapServers("testServer:123");
        kafkaConnector.setOptions(Collections.singletonList(new Option("testOptionName", "testOptionValue")));
        kafkaConnector.setSslTruststorePassword("sslTruststorePassword");
        kafkaConnector.setSslKeystorePassword("sslKeystorePassword");
        kafkaConnector.setSslKeyPassword("sslKeyPassword");
        kafkaConnector.setExternalKafka(Boolean.TRUE);
        kafkaConnector.setSecurityProtocol(SecurityProtocol.SSL);
        kafkaConnector.setSslEndpointIdentificationAlgorithm(SslEndpointIdentificationAlgorithm.EMPTY);

        Mockito.when(storageConnectorApi.getKafkaStorageConnector(Mockito.any(), Mockito.anyBoolean()))
            .thenReturn(kafkaConnector);
        ArgumentCaptor<Boolean> externalArg = ArgumentCaptor.forClass(Boolean.class);

        Map<String, String> writeOptions = new HashMap<>();
        writeOptions.put("testName", "testValue");

        // Act
        Map<String, String> result = kafkaEngine.getKafkaConfig(Mockito.mock(FeatureGroupBase.class), writeOptions, KafkaEngine.ConfigType.SPARK);

        // Assert
        Mockito.verify(storageConnectorApi).getKafkaStorageConnector(Mockito.any(), externalArg.capture());
        Assertions.assertEquals(Boolean.FALSE, externalArg.getValue());
        Assertions.assertEquals("sslTruststorePassword", result.get("kafka.ssl.truststore.password"));
        Assertions.assertEquals("testServer:123", result.get("kafka.bootstrap.servers"));
        Assertions.assertEquals("SSL", result.get("kafka.security.protocol"));
        Assertions.assertEquals("sslKeyPassword", result.get("kafka.ssl.key.password"));
        Assertions.assertEquals("testOptionValue", result.get("kafka.testOptionName"));
        Assertions.assertEquals("", result.get("kafka.ssl.endpoint.identification.algorithm"));
        Assertions.assertEquals("sslKeystorePassword", result.get("kafka.ssl.keystore.password"));
        Assertions.assertEquals("testValue", result.get("testName"));
    }

    @Test
    public void testGetKafkaConfigExternalClient() throws FeatureStoreException, IOException {
        // Arrange
        HopsworksClient hopsworksClient = Mockito.mock(HopsworksClient.class);
        hopsworksClient.setInstance(new HopsworksClient(Mockito.mock(HopsworksHttpClient.class), "host"));

        KafkaEngine kafkaEngine = new KafkaEngine(Mockito.mock(EngineBase.class));
        StorageConnectorApi storageConnectorApi = Mockito.mock(StorageConnectorApi.class);
        kafkaEngine.storageConnectorApi = storageConnectorApi;
        StorageConnector.KafkaConnector kafkaConnector = new StorageConnector.KafkaConnector();
        kafkaConnector.setExternalKafka(Boolean.TRUE);
        kafkaConnector.setSecurityProtocol(SecurityProtocol.SSL);
        kafkaConnector.setSslEndpointIdentificationAlgorithm(SslEndpointIdentificationAlgorithm.EMPTY);

        Mockito.when(storageConnectorApi.getKafkaStorageConnector(Mockito.any(), Mockito.anyBoolean()))
            .thenReturn(kafkaConnector);
        ArgumentCaptor<Boolean> externalArg = ArgumentCaptor.forClass(Boolean.class);

        // Act
        kafkaEngine.getKafkaConfig(Mockito.mock(FeatureGroupBase.class), null, KafkaEngine.ConfigType.SPARK);

        // Assert
        Mockito.verify(storageConnectorApi).getKafkaStorageConnector(Mockito.any(), externalArg.capture());
        Assertions.assertEquals(Boolean.TRUE, externalArg.getValue());
    }

    @Test
    public void testGetKafkaConfigInternalKafka() throws FeatureStoreException, IOException {
        // Arrange
        HopsworksClient hopsworksClient = Mockito.mock(HopsworksClient.class);
        hopsworksClient.setInstance(new HopsworksClient(Mockito.mock(HopsworksHttpClient.class), "host"));
        System.setProperty(HopsworksInternalClient.REST_ENDPOINT_SYS, "");

        KafkaEngine kafkaEngine = new KafkaEngine(Mockito.mock(EngineBase.class));
        StorageConnectorApi storageConnectorApi = Mockito.mock(StorageConnectorApi.class);
        kafkaEngine.storageConnectorApi = storageConnectorApi;
        StorageConnector.KafkaConnector kafkaConnector = new StorageConnector.KafkaConnector();
        kafkaConnector.setExternalKafka(Boolean.TRUE);
        kafkaConnector.setSecurityProtocol(SecurityProtocol.SSL);
        kafkaConnector.setSslEndpointIdentificationAlgorithm(SslEndpointIdentificationAlgorithm.EMPTY);

        Mockito.when(storageConnectorApi.getKafkaStorageConnector(Mockito.any(), Mockito.anyBoolean()))
            .thenReturn(kafkaConnector);
        ArgumentCaptor<Boolean> externalArg = ArgumentCaptor.forClass(Boolean.class);

        Map<String, String> writeOptions = new HashMap<>();
        writeOptions.put("internal_kafka", "true");

        // Act
        kafkaEngine.getKafkaConfig(Mockito.mock(FeatureGroupBase.class), writeOptions, KafkaEngine.ConfigType.SPARK);

        // Assert
        Mockito.verify(storageConnectorApi).getKafkaStorageConnector(Mockito.any(), externalArg.capture());
        Assertions.assertEquals(Boolean.FALSE, externalArg.getValue());
    }

    @Test
    public void testGetKafkaConfigExternalClientInternalKafka() throws FeatureStoreException, IOException {
        // Arrange
        HopsworksClient hopsworksClient = Mockito.mock(HopsworksClient.class);
        hopsworksClient.setInstance(new HopsworksClient(Mockito.mock(HopsworksHttpClient.class), "host"));

        KafkaEngine kafkaEngine = new KafkaEngine(Mockito.mock(EngineBase.class));
        StorageConnectorApi storageConnectorApi = Mockito.mock(StorageConnectorApi.class);
        kafkaEngine.storageConnectorApi = storageConnectorApi;
        StorageConnector.KafkaConnector kafkaConnector = new StorageConnector.KafkaConnector();
        kafkaConnector.setExternalKafka(Boolean.TRUE);
        kafkaConnector.setSecurityProtocol(SecurityProtocol.SSL);
        kafkaConnector.setSslEndpointIdentificationAlgorithm(SslEndpointIdentificationAlgorithm.EMPTY);

        Mockito.when(storageConnectorApi.getKafkaStorageConnector(Mockito.any(), Mockito.anyBoolean()))
            .thenReturn(kafkaConnector);
        ArgumentCaptor<Boolean> externalArg = ArgumentCaptor.forClass(Boolean.class);

        Map<String, String> writeOptions = new HashMap<>();
        writeOptions.put("internal_kafka", "true");

        // Act
        kafkaEngine.getKafkaConfig(Mockito.mock(FeatureGroupBase.class), writeOptions, KafkaEngine.ConfigType.SPARK);

        // Assert
        Mockito.verify(storageConnectorApi).getKafkaStorageConnector(Mockito.any(), externalArg.capture());
        Assertions.assertEquals(Boolean.FALSE, externalArg.getValue());
    }
}
