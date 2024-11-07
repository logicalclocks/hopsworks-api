/*
 *  Copyright (c) 2023. Hopsworks AB
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

package com.logicalclocks.hsfs.spark.engine;

import com.logicalclocks.hsfs.spark.FeatureGroup;
import com.logicalclocks.hsfs.spark.FeatureStore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.List;

public class TestSparkEngine {

    @Test
    public void testConvertToDefaultDataframe() {
        // Arrange
        SparkEngine sparkEngine = SparkEngine.getInstance();

        // Act
        StructType structType = new StructType();
        structType = structType.add("A", DataTypes.StringType, false);
        structType = structType.add("B", DataTypes.StringType, false);
        structType = structType.add("C", DataTypes.StringType, false);
        structType = structType.add("D", DataTypes.StringType, false);

        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create("value1", "value2", "value3", "value4"));

        ArrayList<String> nonNullColumns = new ArrayList<>();
        nonNullColumns.add("a");

        Dataset<Row> dfOriginal = sparkEngine.getSparkSession().createDataFrame(nums, structType);

        Dataset<Row> dfConverted = sparkEngine.convertToDefaultDataframe(dfOriginal);

        StructType expected = new StructType();
        expected = expected.add("a", DataTypes.StringType, true);
        expected = expected.add("b", DataTypes.StringType, true);
        expected = expected.add("c", DataTypes.StringType, true);
        expected = expected.add("d", DataTypes.StringType, true);
        ArrayList<StructField> expectedJava = new ArrayList<>(JavaConverters.asJavaCollection(expected.toSeq()));

        ArrayList<StructField> dfConvertedJava = new ArrayList<>(JavaConverters.asJavaCollection(dfConverted.schema().toSeq()));
        // Assert
        for (int i = 0; i < expectedJava.size(); i++) {
            Assertions.assertEquals(expectedJava.get(i), dfConvertedJava.get(i));
        }

        StructType originalExpected = new StructType();
        originalExpected = originalExpected.add("A", DataTypes.StringType, false);
        originalExpected = originalExpected.add("B", DataTypes.StringType, false);
        originalExpected = originalExpected.add("C", DataTypes.StringType, false);
        originalExpected = originalExpected.add("D", DataTypes.StringType, false);
        ArrayList<StructField> originalExpectedJava = new ArrayList<>(JavaConverters.asJavaCollection(originalExpected.toSeq()));

        ArrayList<StructField> dfOriginalJava = new ArrayList<>(JavaConverters.asJavaCollection(dfOriginal.schema().toSeq()));
        // Assert
        for (int i = 0; i < originalExpectedJava.size(); i++) {
            Assertions.assertEquals(originalExpectedJava.get(i), dfOriginalJava.get(i));
        }
    }

    @Test
    public void testMakeQueryName() {
        SparkEngine sparkEngine = SparkEngine.getInstance();
        FeatureGroup featureGroup = new FeatureGroup();
        Integer fgId = 1;
        String fgName = "test_fg";
        Integer fgVersion = 1;
        Integer projectId = 99;
        featureGroup.setId(fgId);
        featureGroup.setName(fgName);
        featureGroup.setVersion(fgVersion);
        FeatureStore featureStore = (new FeatureStore());
        featureStore.setProjectId(projectId);
        featureGroup.setFeatureStore(featureStore);
        String queryName = String.format("insert_stream_%d_%d_%s_%d_onlinefs",
                featureGroup.getFeatureStore().getProjectId(),
                featureGroup.getId(),
                featureGroup.getName(),
                featureGroup.getVersion()
        );
        // query name is null
        Assertions.assertEquals(queryName, sparkEngine.makeQueryName(null, featureGroup));
        // query name is empty
        Assertions.assertEquals(queryName, sparkEngine.makeQueryName("", featureGroup));
        // query name is not empty
        Assertions.assertEquals("test_qn", sparkEngine.makeQueryName("test_qn", featureGroup));
    }
}
