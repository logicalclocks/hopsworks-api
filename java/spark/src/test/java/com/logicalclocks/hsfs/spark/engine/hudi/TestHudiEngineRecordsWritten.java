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

package com.logicalclocks.hsfs.spark.engine.hudi;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHudiEngineRecordsWritten {

  private static HoodieWriteStat stat(long inserts, long updates, long deletes) {
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setPrevCommit("null");
    stat.setNumInserts(inserts);
    stat.setNumUpdateWrites(updates);
    stat.setNumDeletes(deletes);
    return stat;
  }

  @Test
  void testAnEmptyCommitWroteNothing() {
    Assertions.assertEquals(0, HudiEngine.recordsWritten(new HoodieCommitMetadata()));

    HoodieCommitMetadata noRecords = new HoodieCommitMetadata();
    noRecords.addWriteStat("", stat(0, 0, 0));
    Assertions.assertEquals(0, HudiEngine.recordsWritten(noRecords));
  }

  @Test
  void testInsertsUpdatesAndDeletesAllCount() {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addWriteStat("p=1", stat(3, 0, 0));
    commitMetadata.addWriteStat("p=2", stat(0, 2, 0));
    commitMetadata.addWriteStat("p=3", stat(0, 0, 1));
    Assertions.assertEquals(6, HudiEngine.recordsWritten(commitMetadata));
  }
}
