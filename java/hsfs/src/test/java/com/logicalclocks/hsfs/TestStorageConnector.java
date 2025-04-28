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

package com.logicalclocks.hsfs;

import com.logicalclocks.hsfs.engine.EngineBase;
import com.logicalclocks.hsfs.metadata.Option;

import java.io.IOException;
import java.util.Collections;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
 
 
class TestStorageConnector {
  @Test
  public void testOptions() throws FeatureStoreException, IOException {
    // Arrange
    EngineBase engineBase = Mockito.mock(EngineBase.class);
    Mockito.when(engineBase.addFile(Mockito.anyString())).thenReturn("result_from_add_file");
    EngineBase.setInstance(engineBase);

    StorageConnector.KafkaConnector kafkaConnector = new StorageConnector.KafkaConnector();

    // Act
    kafkaConnector.setOptions(Collections.singletonList(new Option("test", "test")));

    // Assert
    Mockito.verify(engineBase, Mockito.times(0)).addFile(Mockito.anyString());
  }

  @Test
  public void testOptionsSASLAuthentication() throws FeatureStoreException, IOException {
    // Arrange
    EngineBase engineBase = Mockito.mock(EngineBase.class);
    Mockito.when(engineBase.addFile(Mockito.anyString())).thenReturn("result_from_add_file");
    EngineBase.setInstance(engineBase);

    StorageConnector.KafkaConnector kafkaConnector = new StorageConnector.KafkaConnector();

    // Act
    kafkaConnector.setOptions(Collections.singletonList(new Option("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true keyTab=\"/home/laurent/my.keytab\" storeKey=true useTicketCache=false serviceName=\"kafka\" principal=\"laurent@kafka.com\";")));

    // Assert
    Mockito.verify(engineBase, Mockito.times(1)).addFile(Mockito.anyString());
    Assertions.assertEquals("com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true keyTab=\"result_from_add_file\" storeKey=true useTicketCache=false serviceName=\"kafka\" principal=\"laurent@kafka.com\";", kafkaConnector.getOptions().get(0).getValue());
  }
}
 