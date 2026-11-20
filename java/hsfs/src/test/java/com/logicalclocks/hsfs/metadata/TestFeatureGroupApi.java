/*
 *  Copyright (c) 2020-2022. Hopsworks AB
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
package com.logicalclocks.hsfs.metadata;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFeatureGroupApi {

  @Test
  public void testJobExecutionIdOfTheEnvironment() {
    Assertions.assertEquals(42, FeatureGroupApi.jobExecutionId("42"));
  }

  @Test
  public void testNoJobExecutionIdOutsideAJobOrWhenMalformed() {
    for (String value : new String[] {null, "", "abc", "-1", "0", "4.2", " 42", String.valueOf(1L << 31),
        "99999999999999999999"}) {
      Assertions.assertNull(FeatureGroupApi.jobExecutionId(value), value);
    }
  }
}
