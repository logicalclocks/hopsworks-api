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

package com.logicalclocks.hsfs.engine;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.logicalclocks.hsfs.constructor.PreparedStatementParameter;
import com.logicalclocks.hsfs.constructor.ServingPreparedStatement;
import com.logicalclocks.hsfs.metadata.FeatureViewApi;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.HopsworksExternalClient;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.FeatureStoreBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureViewBase;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.metadata.Variable;
import com.logicalclocks.hsfs.metadata.VariablesApi;
import com.logicalclocks.hsfs.util.Constants;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import lombok.NoArgsConstructor;

import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.text.WordUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@NoArgsConstructor
public class VectorServer {

  @Setter
  private HikariDataSource hikariDataSource = null;
  @Getter
  private Map<Integer, TreeMap<String, Integer>> preparedStatementParameters;
  @Getter
  private TreeMap<Integer, ServingPreparedStatement> orderedServingPreparedStatements;
  @Getter
  @Setter
  private HashSet<String> servingKeys;

  private StorageConnectorApi storageConnectorApi = new StorageConnectorApi();
  private FeatureViewApi featureViewApi = new FeatureViewApi();

  private Map<Integer, Map<String, DatumReader<Object>>> featureGroupDatumReaders;
  private Map<Integer, Set<String>> featureGroupFeatures;
  private ExecutorService executorService = Executors.newCachedThreadPool();
  private boolean isBatch = false;
  private VariablesApi variablesApi = new VariablesApi();

  public List<Object> getFeatureVector(FeatureViewBase featureViewBase, Map<String, Object> entry)
      throws FeatureStoreException, IOException, ClassNotFoundException {
    return getFeatureVector(featureViewBase, entry,
        HopsworksClient.getInstance().getHopsworksHttpClient() instanceof HopsworksExternalClient);
  }

  public List<Object> getFeatureVector(FeatureViewBase featureViewBase, Map<String, Object> entry, boolean external)
      throws FeatureStoreException, IOException, ClassNotFoundException {
    if (hikariDataSource == null || isBatch) {
      initPreparedStatement(featureViewBase, false, external);
    }
    checkPrimaryKeys(entry.keySet());
    return getFeatureVector(entry);
  }

  @VisibleForTesting
  public List<Object> getFeatureVector(Map<String, Object> entry)
      throws FeatureStoreException {
    // construct serving vector
    List<Object> servingVector = new ArrayList<>();
    List<Future<List<Object>>> queryFutures = new ArrayList<>();

    for (Integer preparedStatementIndex : orderedServingPreparedStatements.keySet()) {
      queryFutures.add(executorService.submit(() -> {
        try {
          return processQuery(entry, preparedStatementIndex);
        } catch (SQLException | FeatureStoreException | IOException e) {
          throw new RuntimeException(e);
        }
      }));
    }

    for (Future<List<Object>> queryFuture : queryFutures) {
      try {
        servingVector.addAll(queryFuture.get());
      } catch (InterruptedException | ExecutionException e) {
        throw new FeatureStoreException("Error retrieving query statement result", e);
      }
    }

    return servingVector;
  }

  public <T> T getFeatureVectorObject(FeatureViewBase featureViewBase, Map<String, Object> entry, boolean external,
                                       Class<T> returnType)
      throws FeatureStoreException, IOException, ClassNotFoundException,
      IllegalAccessException, InstantiationException {
    if (hikariDataSource == null || isBatch) {
      initPreparedStatement(featureViewBase, false, external);
    }
    checkPrimaryKeys(entry.keySet());
    return getFeatureVectorObject(entry, returnType);
  }

  public <T> T getFeatureVectorObject(Map<String, Object> entry, Class<T> returnType)
      throws FeatureStoreException, InstantiationException, IllegalAccessException {
    List<Future<?>> queryFutures = new ArrayList<>();

    T returnObject = returnType.newInstance();

    for (Integer preparedStatementIndex : orderedServingPreparedStatements.keySet()) {
      queryFutures.add(executorService.submit(() -> {
        try {
          processQuery(entry, preparedStatementIndex, returnObject);
        } catch (SQLException | FeatureStoreException | IOException | NoSuchMethodException
                 | InvocationTargetException | IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }));
    }

    for (Future<?> queryFuture : queryFutures) {
      try {
        queryFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new FeatureStoreException("Error retrieving query statement result", e);
      }
    }

    return returnObject;
  }

  public <T> void processQuery(Map<String, Object> entry, int preparedStatementIndex, T returnObject)
      throws SQLException, FeatureStoreException, IOException,
      NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    try (Connection connection = hikariDataSource.getConnection()) {
      // Create the prepared statement
      ServingPreparedStatement servingPreparedStatement = orderedServingPreparedStatements.get(preparedStatementIndex);

      PreparedStatement preparedStatement = connection.prepareStatement(servingPreparedStatement.getQueryOnline());

      // Set the parameters base do the entry object
      Map<String, Integer> parameterIndexInStatement = preparedStatementParameters.get(preparedStatementIndex);
      for (String name : entry.keySet()) {
        if (parameterIndexInStatement.containsKey(name)) {
          preparedStatement.setObject(parameterIndexInStatement.get(name), entry.get(name));
        }
      }

      ResultSet results = preparedStatement.executeQuery();
      // check if results contain any data at all and throw exception if not
      if (!results.isBeforeFirst()) {
        throw new FeatureStoreException("No data was retrieved from online feature store.");
      }
      //Get column count
      int columnCount = results.getMetaData().getColumnCount();

      // get the complex schema datum readers for this feature group
      Map<String, DatumReader<Object>> featuresDatumReaders =
          featureGroupDatumReaders.get(servingPreparedStatement.getFeatureGroupId());

      //append results to servingVector
      while (results.next()) {
        int index = 1;
        while (index <= columnCount) {
          String methodName = "set" + WordUtils.capitalize(results.getMetaData().getColumnLabel(index));
          Object columnValue;
          if (featuresDatumReaders != null
              && featuresDatumReaders.containsKey(results.getMetaData().getColumnLabel(index))) {
            columnValue =
                deserializeComplexFeature(featuresDatumReaders.get(results.getMetaData().getColumnLabel(index)),
                    results, index);
          } else {
            columnValue = results.getObject(index);
          }
          Method setter = returnObject.getClass().getDeclaredMethod(methodName, columnValue.getClass());
          setter.invoke(returnObject, columnValue);
          index++;
        }
      }
      results.close();
    }
  }


  private List<Object> processQuery(Map<String, Object> entry, int preparedStatementIndex)
      throws SQLException, FeatureStoreException, IOException {
    List<Object> servingVector = new ArrayList<>();
    try (Connection connection = hikariDataSource.getConnection()) {
      // Create the prepared statement
      ServingPreparedStatement servingPreparedStatement = orderedServingPreparedStatements.get(preparedStatementIndex);

      PreparedStatement preparedStatement = connection.prepareStatement(servingPreparedStatement.getQueryOnline());

      // Set the parameters base do the entry object
      Map<String, Integer> parameterIndexInStatement = preparedStatementParameters.get(preparedStatementIndex);
      for (String name : entry.keySet()) {
        if (parameterIndexInStatement.containsKey(name)) {
          preparedStatement.setObject(parameterIndexInStatement.get(name), entry.get(name));
        }
      }

      ResultSet results = preparedStatement.executeQuery();
      // check if results contain any data at all and throw exception if not
      if (!results.isBeforeFirst()) {
        throw new FeatureStoreException("No data was retrieved from online feature store.");
      }
      //Get column count
      int columnCount = results.getMetaData().getColumnCount();

      // get the complex schema datum readers for this feature group
      Map<String, DatumReader<Object>> featuresDatumReaders =
          featureGroupDatumReaders.get(servingPreparedStatement.getFeatureGroupId());

      //append results to servingVector
      while (results.next()) {
        int index = 1;
        while (index <= columnCount) {
          if (featuresDatumReaders != null
              && featuresDatumReaders.containsKey(results.getMetaData().getColumnLabel(index))) {
            servingVector.add(
                deserializeComplexFeature(
                    featuresDatumReaders.get(results.getMetaData().getColumnLabel(index)), results, index));
          } else {
            servingVector.add(results.getObject(index));
          }
          index++;
        }
      }
      results.close();
    }

    return servingVector;
  }

  public List<List<Object>> getFeatureVectors(FeatureViewBase featureViewBase, Map<String, List<Object>> entry,
                                              boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    if (hikariDataSource == null || !isBatch) {
      initPreparedStatement(featureViewBase, true, external);
    }
    return getFeatureVectors(entry);
  }

  public List<List<Object>> getFeatureVectors(Map<String, List<Object>> entry)
      throws SQLException, FeatureStoreException, IOException {
    checkPrimaryKeys(entry.keySet());
    List<String> queries = Lists.newArrayList();
    for (Integer fgId : orderedServingPreparedStatements.keySet()) {
      String query = orderedServingPreparedStatements.get(fgId).getQueryOnline();
      String zippedTupleString =
          zipArraysToTupleString(preparedStatementParameters.get(fgId)
            .entrySet()
            .stream()
            .sorted(Comparator.comparingInt(Map.Entry::getValue))
            .map(e -> entry.get(e.getKey()))
            .collect(Collectors.toList()));
      queries.add(query.replaceFirst("\\?", zippedTupleString));
    }

    return getFeatureVectors(queries, entry);
  }

  private List<List<Object>> getFeatureVectors(List<String> queries, Map<String, List<Object>> entry)
      throws SQLException, FeatureStoreException, IOException {
    ArrayList<Object> servingVector = new ArrayList<>();

    // construct batch of serving vectors
    // Create map object that will have of order of the vector as key and values as
    // vector itself to stitch them correctly if there are multiple feature groups involved. At this point we
    // expect that backend will return correctly ordered vectors.
    int batchSize = entry.values().stream().findAny().get().size();
    List<List<Object>> servingVectorArray = new ArrayList<>(batchSize);
    for (int i = 0; i < batchSize; i++) {
      servingVectorArray.add(new ArrayList<>());
    }

    try (Connection connection = hikariDataSource.getConnection()) {
      try (Statement stmt = connection.createStatement()) {
        // Used to reference the ServingPreparedStatement for deserialization
        int statementOrder = 0;
        for (String query : queries) {

          // MySQL doesn't support setting array type on prepared statement. This is the hack to replace
          // the ? with array joined as comma separated array.
          try (ResultSet results = stmt.executeQuery(query)) {

            // check if results contain any data at all and throw exception if not
            if (!results.isBeforeFirst()) {
              throw new FeatureStoreException("No data was retrieved from online feature store.");
            }
            //Get column count
            int columnCount = results.getMetaData().getColumnCount();

            // get the complex schema datum readers for this feature group
            ServingPreparedStatement servingPreparedStatement = orderedServingPreparedStatements.get(statementOrder);
            Map<String, DatumReader<Object>> featuresDatumReaders =
                featureGroupDatumReaders.get(servingPreparedStatement.getFeatureGroupId());
            Set<String> selectedFeatureNames = featureGroupFeatures.get(servingPreparedStatement.getFeatureGroupId());

            //append results to servingVector
            while (results.next()) {
              int index = 1;
              while (index <= columnCount) {
                // for get batch data, the query is also returning the primary key to be able to sort the results
                // if the primary keys have not being selected, we should filter them out here.
                if (!selectedFeatureNames.contains(results.getMetaData().getColumnLabel(index))) {
                  index++;
                  continue;
                }

                if (featuresDatumReaders != null
                    && featuresDatumReaders.containsKey(results.getMetaData().getColumnLabel(index))) {
                  servingVector.add(
                      deserializeComplexFeature(
                          featuresDatumReaders.get(results.getMetaData().getColumnLabel(index)), results, index));
                } else {
                  servingVector.add(results.getObject(index));
                }
                index++;
              }

              List<Integer> orderInBatch = getOrderInBatch(results, entry, statementOrder, batchSize);

              // get vector by order and update with vector from other feature group(s)
              for (Integer order : orderInBatch) {
                servingVectorArray.get(order).addAll(servingVector);
              }

              // empty servingVector for new primary key
              servingVector = new ArrayList<>();
            }
          }
          statementOrder++;
        }
      }
    }
    return servingVectorArray;
  }

  public void initServing(FeatureViewBase featureViewBase, boolean batch)
      throws FeatureStoreException, IOException, ClassNotFoundException {
    initPreparedStatement(featureViewBase, batch);
  }

  public void initServing(FeatureViewBase featureViewBase, boolean batch, boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    initPreparedStatement(featureViewBase, batch, external);
  }

  public void initPreparedStatement(FeatureViewBase featureViewBase, boolean batch)
      throws FeatureStoreException, IOException, ClassNotFoundException {
    initPreparedStatement(featureViewBase, batch, HopsworksClient.getInstance().getHopsworksHttpClient()
        instanceof HopsworksExternalClient);
  }

  public void initPreparedStatement(FeatureViewBase featureViewBase, boolean batch, boolean external)
      throws FeatureStoreException, IOException, ClassNotFoundException {
    List<ServingPreparedStatement> servingPreparedStatements =
        featureViewApi.getServingPreparedStatement(featureViewBase, batch);
    initPreparedStatement(featureViewBase.getFeatureStore(), featureViewBase.getFeatures(),
        servingPreparedStatements, batch, external);
  }

  @VisibleForTesting
  public void initPreparedStatement(FeatureStoreBase featureStoreBase,
                                     List<TrainingDatasetFeature> features,
                                     List<ServingPreparedStatement> servingPreparedStatements,
                                     boolean batch,
                                     boolean external)
      throws FeatureStoreException, IOException, ClassNotFoundException {

    this.isBatch = batch;
    setupHikariPool(featureStoreBase, external);
    // map of prepared statement index and its corresponding parameter indices
    Map<Integer, TreeMap<String, Integer>> preparedStatementParameters = new HashMap<>();

    // in case its batch serving then we need to save sql string only
    TreeMap<Integer, ServingPreparedStatement> orderedServingPreparedStatements = new TreeMap<>();

    // save unique primary key names that will be used by user to retrieve serving vector
    HashSet<String> servingVectorKeys = new HashSet<>();

    // Prefix map (featureGroupId -> prefix) to compute feature names
    Map<Integer, String> prefixMap = new HashMap<>();

    for (ServingPreparedStatement servingPreparedStatement : servingPreparedStatements) {
      prefixMap.put(servingPreparedStatement.getFeatureGroupId(), servingPreparedStatement.getPrefix());
      orderedServingPreparedStatements.put(servingPreparedStatement.getPreparedStatementIndex(),
          servingPreparedStatement);
      TreeMap<String, Integer> parameterIndices = new TreeMap<>();
      servingPreparedStatement.getPreparedStatementParameters().forEach(preparedStatementParameter -> {
        servingVectorKeys.add(preparedStatementParameter.getName());
        parameterIndices.put(preparedStatementParameter.getName(), preparedStatementParameter.getIndex());
      });
      preparedStatementParameters.put(servingPreparedStatement.getPreparedStatementIndex(), parameterIndices);
    }
    this.servingKeys = servingVectorKeys;

    this.preparedStatementParameters = preparedStatementParameters;
    this.orderedServingPreparedStatements = orderedServingPreparedStatements;
    this.featureGroupDatumReaders = getComplexFeatureSchemas(features, prefixMap);
    this.featureGroupFeatures = features.stream()
        .collect(Collectors.groupingBy(
            feature -> feature.getFeaturegroup().getId(),
            Collectors.mapping(
                TrainingDatasetFeature::getName,
                Collectors.toSet()
            )
        ));
  }

  @VisibleForTesting
  public void setupHikariPool(FeatureStoreBase featureStoreBase, Boolean external)
      throws FeatureStoreException, IOException, ClassNotFoundException {
    Class.forName("com.mysql.cj.jdbc.Driver");

    StorageConnector.JdbcConnector storageConnectorBase =
        storageConnectorApi.getOnlineStorageConnector(featureStoreBase, StorageConnector.JdbcConnector.class);
    Map<String, String> jdbcOptions = storageConnectorBase.sparkOptions();
    String url = jdbcOptions.get(Constants.JDBC_URL);
    if (external) {
      // if external is true, replace the IP coming from the storage connector with the host
      // used during the connection setup
      String host;
      Optional<Variable> loadbalancerVariable = variablesApi.get(VariablesApi.LOADBALANCER_EXTERNAL_DOMAIN_MYSQL);
      if (loadbalancerVariable.isPresent() && !Strings.isNullOrEmpty(loadbalancerVariable.get().getValue())) {
        host = loadbalancerVariable.get().getValue();
      } else {
        // Fall back to the mysql server on the head node
        throw new FeatureStoreException("No external domain for MySQL was found in the "
        + "Hopsworks Cluster Configuration variables. Contact your administrator.");
      }

      url = url.replaceAll("/[0-9.]+:", "/" + host + ":");
    }

    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(url);
    config.setUsername(jdbcOptions.get(Constants.JDBC_USER));
    config.setPassword(jdbcOptions.get(Constants.JDBC_PWD));
    hikariDataSource = new HikariDataSource(config);
  }

  @VisibleForTesting
  public String zipArraysToTupleString(List<List<Object>> lists) {
    List<String> zippedTuples = new ArrayList<>();
    for (int i = 0; i < lists.get(0).size(); i++) {
      List<String> zippedArray = new ArrayList<String>();
      for (List<Object> in : lists) {
        if (in.get(i) instanceof String) {
          zippedArray.add("'" + in.get(i).toString() + "'");
        } else {
          zippedArray.add(in.get(i).toString());
        }
      }
      zippedTuples.add("(" + String.join(",", zippedArray) + ")");
    }
    return "(" + String.join(",", zippedTuples) + ")";
  }

  private Object deserializeComplexFeature(DatumReader<Object> featureDatumReader, ResultSet results,
                                           int index) throws SQLException, IOException {
    if (results.getBytes(index) != null) {
      Decoder decoder = DecoderFactory.get().binaryDecoder(results.getBytes(index), null);
      return featureDatumReader.read(null, decoder);
    } else {
      return null;
    }
  }

  private List<Integer> getOrderInBatch(ResultSet results, Map<String, List<Object>> entry, int statementOrder,
                                        int batchSize)
      throws SQLException {
    // This accounts for partial keys with duplicates
    List<Integer> orderInBatch = new ArrayList<>();

    // get the first prepared statement parameter for this statement
    ServingPreparedStatement preparedStatement = orderedServingPreparedStatements.get(statementOrder);

    for (int i = 0; i < batchSize; i++) {
      boolean correctIndex = false;

      for (PreparedStatementParameter preparedStatementParameter : preparedStatement.getPreparedStatementParameters()) {
        List<Object> entryForParameter = entry.get(preparedStatementParameter.getName());
        Class expectedResultClass = entryForParameter.get(i).getClass();

        String columnName = Strings.isNullOrEmpty(preparedStatement.getPrefix())
            ? preparedStatementParameter.getName()
            : preparedStatement.getPrefix() + preparedStatementParameter.getName();

        if (results.getObject(columnName, expectedResultClass).equals(entryForParameter.get(i))) {
          correctIndex = true;
        } else {
          correctIndex = false;
          break;
        }
      }

      if (correctIndex) {
        orderInBatch.add(i);
      }
    }

    return orderInBatch;
  }

  @VisibleForTesting
  public Map<Integer, Map<String, DatumReader<Object>>> getComplexFeatureSchemas(List<TrainingDatasetFeature> features,
                                                                                 Map<Integer, String> prefixMap)
      throws FeatureStoreException, IOException {
    Map<Integer, Map<String, DatumReader<Object>>> featureSchemaMap = new HashMap<>();
    for (TrainingDatasetFeature f : features) {
      if (f.isComplex()) {
        Map<String, DatumReader<Object>> featureGroupMap = featureSchemaMap.get(f.getFeaturegroup().getId());
        if (featureGroupMap == null) {
          featureGroupMap = new HashMap<>();
        }

        // Need to remove the prefix here as we are looking up the feature group metadata
        // which doesn't have knowledge of prefixes.
        String featureName = f.getName();
        String prefix = prefixMap.get(f.getFeaturegroup().getId());
        if (!Strings.isNullOrEmpty(prefix) && featureName.startsWith(prefix)) {
          featureName = featureName.substring(prefix.length());
        }

        DatumReader<Object> datumReader =
            new GenericDatumReader<>(new Schema.Parser().parse(f.getFeaturegroup().getFeatureAvroSchema(featureName)));
        featureGroupMap.put(f.getName(), datumReader);

        featureSchemaMap.put(f.getFeaturegroup().getId(), featureGroupMap);
      }
    }
    return featureSchemaMap;
  }

  private void checkPrimaryKeys(Set<String> primaryKeys) {
    //check if primary key map correspond to serving_keys.
    if (!servingKeys.equals(primaryKeys)) {
      throw new IllegalArgumentException("Provided primary key map doesn't correspond to serving_keys");
    }
  }

  public void close() {
    hikariDataSource.close();
    executorService.shutdown();
  }
}
