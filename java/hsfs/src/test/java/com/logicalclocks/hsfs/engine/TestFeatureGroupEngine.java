/*
 *  Copyright (c) 2026. Hopsworks AB
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

import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;

class TestFeatureGroupEngine {

  private FeatureGroupEngine engine;
  private FeatureGroupApi mockApi;
  private FeatureStore mockFeatureStore;

  @BeforeEach
  void setUp() throws Exception {
    engine = new FeatureGroupEngine();
    mockApi = Mockito.mock(FeatureGroupApi.class);
    mockFeatureStore = Mockito.mock(FeatureStore.class);

    Field apiField = FeatureGroupEngineBase.class.getDeclaredField("featureGroupApi");
    apiField.setAccessible(true);
    apiField.set(engine, mockApi);
  }

  @Test
  void testGetOrCreateReturnsFgWhenBackendReturns404NotFound() throws Exception {
    // Backend returns 404 with errorCode 270009 (feature group not found)
    Mockito.when(mockApi.getInternal(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenThrow(new IOException("Error: 404{\"errorCode\":270009,\"errorMsg\":\"Featuregroup wasn't found.\"}"));

    StreamFeatureGroup result = engine.getOrCreateFeatureGroup(
        mockFeatureStore, "test_fg", 1, "desc", true,
        TimeTravelFormat.HUDI, Collections.singletonList("id"),
        Collections.emptyList(), null, "id", Collections.emptyList(),
        null, null, null, null);

    Assert.assertNotNull(result);
    Assert.assertEquals("test_fg", result.getName());
    Assert.assertEquals(Integer.valueOf(1), result.getVersion());
    Assert.assertEquals(true, result.getOnlineEnabled());
  }

  @Test
  void testGetStreamFeatureGroupReturnsExistingFg() throws Exception {
    // Backend returns an existing feature group
    StreamFeatureGroup existing = new StreamFeatureGroup();
    existing.setName("existing_fg");
    existing.setVersion(1);
    Mockito.when(mockApi.getInternal(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(new StreamFeatureGroup[]{existing});

    StreamFeatureGroup result = engine.getStreamFeatureGroup(mockFeatureStore, "existing_fg", 1);

    Assert.assertNotNull(result);
    Assert.assertEquals("existing_fg", result.getName());
    Assert.assertEquals(Integer.valueOf(1), result.getVersion());
  }

  @Test
  void testGetOrCreateRethrowsUnrelatedExceptions() {
    // Errors unrelated to "not found" should propagate
    try {
      Mockito.when(mockApi.getInternal(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
          .thenThrow(new IOException("Error: 500{\"errorCode\":500000,\"errorMsg\":\"Internal server error.\"}"));

      engine.getOrCreateFeatureGroup(
          mockFeatureStore, "test_fg", 1, "desc", true,
          TimeTravelFormat.HUDI, Collections.singletonList("id"),
          Collections.emptyList(), null, "id", Collections.emptyList(),
          null, null, null, null);

      Assert.fail("Expected IOException to be rethrown");
    } catch (IOException | com.logicalclocks.hsfs.FeatureStoreException e) {
      Assert.assertTrue(e.getMessage().contains("500000"));
    } catch (Exception e) {
      Assert.fail("Unexpected exception type: " + e.getClass());
    }
  }
}
