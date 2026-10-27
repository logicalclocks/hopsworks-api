/*
 *  Copyright (c) 2022-2023. Hopsworks AB
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

package com.logicalclocks.hsfs.spark;

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Project;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.HopsworksHttpClient;
import com.logicalclocks.hsfs.spark.constructor.Query;
import com.logicalclocks.hsfs.spark.engine.FeatureGroupEngine;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

public class TestFeatureGroup {

  @Test
  public void testSelect_fgId_set() {
    FeatureGroup featureGroup = new FeatureGroup(null, 1);
    List<String> selectFeatures = Arrays.asList("ft1", "ft2");

    Query result = featureGroup.select(selectFeatures);
    Assertions.assertEquals(1, result.getLeftFeatures().get(0).getFeatureGroupId());
    Assertions.assertEquals(1, result.getLeftFeatures().get(1).getFeatureGroupId());
  }

  @Test
  public void testFeatureGroupPrimaryKey() {
    // Arrange
    FeatureStore featureStore = Mockito.mock(FeatureStore.class);
    FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();

    // Act
    List<Feature> features = new ArrayList<>();
    features.add( new Feature("featureA"));
    features.add( new Feature("featureB"));
    features.add( new Feature("featureC"));

    StreamFeatureGroup featureGroup = new StreamFeatureGroup(featureStore, "fgName", 1, "description",
        Collections.singletonList("primaryKey"), Collections.singletonList("partitionKey"), "hudiPrecombineKey",
        true, TimeTravelFormat.HUDI, features, null, "onlineTopicName", null, null, null, null, null, null);

    Exception pkException = assertThrows(FeatureStoreException.class, () -> {
      featureGroupEngine.saveFeatureGroupMetaData(featureGroup,
         null, null, null, null, null);
    });

    // Assert
    Assertions.assertEquals(pkException.getMessage(),
        "Provided primary key(s) primarykey doesn't exist in feature dataframe");
  }

  @Test
  public void testFeatureGroupEventTimeFeature() {
    // Arrange
    FeatureStore featureStore = Mockito.mock(FeatureStore.class);
    FeatureGroupEngine streamFeatureGroupEngine = new FeatureGroupEngine();

    // Act
    List<Feature> features = new ArrayList<>();
    features.add( new Feature("featureA"));
    features.add( new Feature("featureB"));
    features.add( new Feature("featureC"));

    StreamFeatureGroup featureGroup = new StreamFeatureGroup(featureStore, "fgName", 1, "description",
        Collections.singletonList("featureA"), null, null,
        true, TimeTravelFormat.HUDI, features, null, "onlineTopicName", null, null, "eventTime", null, null, null);

    Exception eventTimeException = assertThrows(FeatureStoreException.class, () -> {
      streamFeatureGroupEngine.saveFeatureGroupMetaData(featureGroup,
          null, null, null, null, null);
    });

    // Assert
    Assertions.assertEquals(eventTimeException.getMessage(),
        "Provided eventTime feature name eventTime doesn't exist in feature dataframe");
  }

  @Test
  public void testFeatureGroupPartitionPrecombineKeys() {
    // Arrange
    FeatureStore featureStore = Mockito.mock(FeatureStore.class);
    FeatureGroupEngine streamFeatureGroupEngine = new FeatureGroupEngine();

    // Act
    List<Feature> features = new ArrayList<>();
    features.add( new Feature("featureA"));
    features.add( new Feature("featureB"));
    features.add( new Feature("featureC"));

    StreamFeatureGroup featureGroup = new StreamFeatureGroup(featureStore, "fgName", 1, "description",
        Collections.singletonList("featureA"), Collections.singletonList("partitionKey"), "hudiPrecombineKey",
        true, TimeTravelFormat.HUDI, features, null, "onlineTopicName", null, null, null, null, null, null);

    Exception partitionException = assertThrows(FeatureStoreException.class, () -> {
      streamFeatureGroupEngine.saveFeatureGroupMetaData(featureGroup,
          Collections.singletonList("partitionKey"), null, null, null,
          null);
    });

    Exception precombineException = assertThrows(FeatureStoreException.class, () -> {
      streamFeatureGroupEngine.saveFeatureGroupMetaData(featureGroup,
          null, "hudiPrecombineKey", null, null,
          null);
    });

    // Assert
    Assertions.assertEquals(partitionException.getMessage(),
        "Provided partition key(s) partitionKey doesn't exist in feature dataframe");

    Assertions.assertEquals(precombineException.getMessage(),
        "Provided Hudi precombine key hudiPrecombineKey doesn't exist in feature dataframe");
  }

  @Test
  public void testFeatureGroupAppendFeaturesResetSubject() throws FeatureStoreException, IOException, ParseException {
    // Arrange
    HopsworksClient hopsworksClient = Mockito.mock(HopsworksClient.class);
    Mockito.when(hopsworksClient.getProject()).thenReturn(new Project(1));
    HopsworksClient.setInstance(hopsworksClient);

    FeatureStore featureStore = Mockito.mock(FeatureStore.class);
    FeatureGroupApi featureGroupApi = Mockito.mock(FeatureGroupApi.class);
    FeatureGroupEngine featureGroupEngine = Mockito.mock(FeatureGroupEngine.class);

    FeatureGroupBase featureGroupBase = Mockito.mock(FeatureGroupBase.class);
    when(featureGroupBase.getFeatures()).thenReturn(new ArrayList<>());
    when(featureGroupApi.updateMetadata(Mockito.any(), Mockito.anyString(), Mockito.any()))
        .thenReturn(featureGroupBase);

    List<Feature> features = new ArrayList<>();
    features.add( new Feature("featureA"));
    features.add( new Feature("featureB"));
    features.add( new Feature("featureC"));

    StreamFeatureGroup featureGroup = new StreamFeatureGroup(featureStore, "fgName", 1, "description",
        Collections.singletonList("featureA"), null, null,
        true, TimeTravelFormat.HUDI, features, null, "onlineTopicName", null, null, "eventTime", null, null, null);
    featureGroup.featureGroupEngine = featureGroupEngine;

    // Act
    featureGroup.appendFeatures(new Feature("featureD"));

    // Assert
    Assertions.assertNull(featureGroup.getSubject());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCommitDeleteRecordDelegatesToRemoveRowsOfflineOnly() throws Exception {
    // The deprecated commitDeleteRecord overloads must delegate to removeRows and delete from
    // the offline table only. They exist for callers written before the online delete, so
    // neither may reach the online store; removeRows(Storage) is where that choice lives.
    FeatureGroup featureGroup = Mockito.spy(new FeatureGroup(null, 1));
    Dataset<Row> df = Mockito.mock(Dataset.class);
    Map<String, String> writeOptions = new HashMap<>();

    Mockito.doNothing().when(featureGroup).removeRows(Mockito.eq(df), Mockito.any(Storage.class));
    Mockito.doNothing().when(featureGroup)
        .removeRows(Mockito.eq(df), Mockito.eq(writeOptions), Mockito.any(Storage.class));

    featureGroup.commitDeleteRecord(df);
    featureGroup.commitDeleteRecord(df, writeOptions);

    Mockito.verify(featureGroup).removeRows(df, Storage.OFFLINE);
    Mockito.verify(featureGroup).removeRows(df, writeOptions, Storage.OFFLINE);
  }

  @Test
  public void testRemoveRowsPassesNullStorage() throws Exception {
    // The overloads without a storage argument must pass null rather than resolving the
    // feature group themselves: null is what tells commitDelete to follow the feature group,
    // and the resolution lives there so both feature group classes share one copy of it.
    FeatureGroupEngine featureGroupEngine = Mockito.mock(FeatureGroupEngine.class);
    Dataset<Row> df = Mockito.mock(Dataset.class);
    Map<String, String> writeOptions = new HashMap<>();

    FeatureGroup offlineOnly = new FeatureGroup(null, 1);
    offlineOnly.featureGroupEngine = featureGroupEngine;
    offlineOnly.setOnlineEnabled(false);

    offlineOnly.removeRows(df);
    offlineOnly.removeRows(df, writeOptions);

    Mockito.verify(featureGroupEngine).commitDelete(offlineOnly, df, null, null);
    Mockito.verify(featureGroupEngine).commitDelete(offlineOnly, df, writeOptions, null);

    FeatureGroup onlineEnabled = new FeatureGroup(null, 2);
    onlineEnabled.featureGroupEngine = featureGroupEngine;
    onlineEnabled.setOnlineEnabled(true);

    onlineEnabled.removeRows(df);

    Mockito.verify(featureGroupEngine).commitDelete(onlineEnabled, df, null, null);
  }

  @Test
  public void testStreamFeatureGroupRemoveRowsPassesNullStorage() throws Exception {
    // Same contract on the stream feature group, whose overloads carry their own copy of the
    // call into commitDelete.
    FeatureGroupEngine featureGroupEngine = Mockito.mock(FeatureGroupEngine.class);
    Dataset<Row> df = Mockito.mock(Dataset.class);
    Map<String, String> writeOptions = new HashMap<>();

    StreamFeatureGroup offlineOnly = new StreamFeatureGroup(null, 1);
    offlineOnly.featureGroupEngine = featureGroupEngine;
    offlineOnly.setOnlineEnabled(false);

    offlineOnly.removeRows(df);
    offlineOnly.removeRows(df, writeOptions);

    Mockito.verify(featureGroupEngine).commitDelete(offlineOnly, df, null, null);
    Mockito.verify(featureGroupEngine).commitDelete(offlineOnly, df, writeOptions, null);

    StreamFeatureGroup onlineEnabled = new StreamFeatureGroup(null, 2);
    onlineEnabled.featureGroupEngine = featureGroupEngine;
    onlineEnabled.setOnlineEnabled(true);

    onlineEnabled.removeRows(df);

    Mockito.verify(featureGroupEngine).commitDelete(onlineEnabled, df, null, null);
  }
}
