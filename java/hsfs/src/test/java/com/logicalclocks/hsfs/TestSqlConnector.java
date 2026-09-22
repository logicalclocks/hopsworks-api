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

package com.logicalclocks.hsfs;

import com.logicalclocks.hsfs.StorageConnector.SqlConnector;
import com.logicalclocks.hsfs.metadata.Option;
import com.logicalclocks.hsfs.util.Constants;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

class TestSqlConnector {

  private SqlConnector teradataConnector() {
    SqlConnector sc = new SqlConnector();
    sc.setStorageConnectorType(StorageConnectorType.SQL);
    sc.setDatabaseType("TERADATA");
    sc.setHost("teradata.example.com");
    sc.setPort(1025);
    sc.setDatabase("demo_user");
    sc.setUser("demo_user");
    sc.setPassword("secret");
    return sc;
  }

  @Test
  void testTeradataUrlCarriesDatabaseAndPortAsParameters() throws FeatureStoreException {
    // Teradata has no host:port/database form; the shape every other engine uses would not parse.
    Map<String, String> options = teradataConnector().sparkOptions(null);

    assertEquals("jdbc:teradata://teradata.example.com/DATABASE=demo_user,DBS_PORT=1025",
        options.get(Constants.JDBC_URL));
    assertEquals("com.teradata.jdbc.TeraDriver", options.get(Constants.JDBC_DRIVER));
  }

  @Test
  void testArgumentsCannotOverrideConnectorOwnedOptions() throws FeatureStoreException {
    // A free-form argument must not be able to point the read at another server or run it as
    // another identity. The connector's own fields own these four keys.
    SqlConnector sc = teradataConnector();
    sc.setArguments(Arrays.asList(
        new Option(Constants.JDBC_URL, "jdbc:teradata://attacker.example.com/DATABASE=x"),
        new Option(Constants.JDBC_USER, "someone_else"),
        new Option(Constants.JDBC_PWD, "not_the_password"),
        new Option(Constants.JDBC_DRIVER, "org.example.Driver"),
        new Option("LOGMECH", "LDAP")));

    Map<String, String> options = sc.sparkOptions(null);

    assertEquals("jdbc:teradata://teradata.example.com/DATABASE=demo_user,DBS_PORT=1025",
        options.get(Constants.JDBC_URL));
    assertEquals("demo_user", options.get(Constants.JDBC_USER));
    assertEquals("secret", options.get(Constants.JDBC_PWD));
    assertEquals("com.teradata.jdbc.TeraDriver", options.get(Constants.JDBC_DRIVER));
    // A non-reserved argument still reaches the driver.
    assertEquals("LDAP", options.get("LOGMECH"));
  }

  @Test
  void testConnectionArgumentsAreNotForwardedAsJdbcProperties() throws FeatureStoreException {
    // Spark hands an option it does not recognise to the driver as a connection property, so a
    // host, port, dbs_port or database argument would contradict the URL built from the fields.
    SqlConnector sc = teradataConnector();
    sc.setArguments(Arrays.asList(
        new Option("host", "attacker.example.com"),
        new Option("DBS_PORT", "9999"),
        new Option(" database ", "other_db"),
        new Option("database_type", "MYSQL"),
        new Option("LOGMECH", "LDAP")));

    Map<String, String> options = sc.sparkOptions(null);

    assertFalse(options.containsKey("host"));
    assertFalse(options.containsKey("DBS_PORT"));
    assertFalse(options.containsKey("database"));
    assertFalse(options.containsKey("database_type"));
    assertEquals("jdbc:teradata://teradata.example.com/DATABASE=demo_user,DBS_PORT=1025",
        options.get(Constants.JDBC_URL));
    assertEquals("LDAP", options.get("LOGMECH"));
  }

  @Test
  void testUnsupportedDatabaseTypeIsRefused() {
    SqlConnector sc = teradataConnector();
    sc.setDatabaseType("NOT_A_DATABASE");

    FeatureStoreException e =
        assertThrows(FeatureStoreException.class, () -> sc.sparkOptions(null));
    assertTrue(e.getMessage().contains("NOT_A_DATABASE"));
  }
}
