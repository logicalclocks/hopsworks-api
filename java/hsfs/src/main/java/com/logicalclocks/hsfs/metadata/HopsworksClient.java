/*
 *  Copyright (c) 2020-2023. Hopsworks AB
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

import com.damnhandy.uri.template.UriTemplate;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Project;
import com.logicalclocks.hsfs.SecretStore;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Base64;

public class HopsworksClient {

  public static final String API_PATH = "/hopsworks-api/api";
  public static final String PROJECT_PATH = API_PATH + "/project{/projectId}";

  protected static HopsworksClient hopsworksClientInstance = null;
  protected static final Logger LOGGER = LoggerFactory.getLogger(HopsworksClient.class);

  @Getter
  @Setter
  protected Project project;
  @Getter
  protected String host;

  public static HopsworksClient getInstance() throws FeatureStoreException {
    if (hopsworksClientInstance == null) {
      throw new FeatureStoreException("Client not connected. Please establish a Hopsworks connection first");
    }
    return hopsworksClientInstance;
  }

  // For testing
  public static void setInstance(HopsworksClient instance) {
    hopsworksClientInstance = instance;
  }

  public static synchronized HopsworksClient setupHopsworksClient(String host, int port, Region region,
                                                                  SecretStore secretStore, boolean hostnameVerification,
                                                                  String trustStorePath, String apiKeyFilePath,
                                                                  String apiKeyValue)
      throws FeatureStoreException {
    if (hopsworksClientInstance != null) {
      return hopsworksClientInstance;
    }

    HopsworksHttpClient hopsworksHttpClient = null;
    try {
      if (System.getProperties().containsKey(HopsworksInternalClient.REST_ENDPOINT_SYS)) {
        hopsworksHttpClient = new HopsworksInternalClient();
      } else {
        hopsworksHttpClient = new HopsworksExternalClient(host, port, region,
            secretStore, hostnameVerification, trustStorePath, apiKeyFilePath, apiKeyValue);
      }
    } catch (Exception e) {
      throw new FeatureStoreException("Could not setup Hopsworks client", e);
    }

    hopsworksClientInstance = new HopsworksClient(hopsworksHttpClient, host);
    return hopsworksClientInstance;
  }

  public Credentials getCredentials() throws FeatureStoreException, IOException,
      KeyStoreException, CertificateException, NoSuchAlgorithmException {
    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH
        + "/credentials";

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", hopsworksClient.getProject().getProjectId())
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    Credentials credentials = hopsworksClient.handleRequest(new HttpGet(uri), Credentials.class);

    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(new ByteArrayInputStream(Base64.getDecoder().decode(credentials.getkStore())),
        credentials.getPassword().toCharArray());
    File keyStore = createTempFile("keyStore.jks");
    ks.store(new FileOutputStream(keyStore), credentials.getPassword().toCharArray());

    KeyStore ts =  KeyStore.getInstance("JKS");
    ts.load(new ByteArrayInputStream(Base64.getDecoder().decode(credentials.gettStore())),
        credentials.getPassword().toCharArray());
    File trustStore = createTempFile("trustStore.jks");
    ts.store(new FileOutputStream(trustStore), credentials.getPassword().toCharArray());

    credentials.setkStore(keyStore.getAbsolutePath());
    credentials.settStore(trustStore.getAbsolutePath());

    return credentials;
  }

  private File createTempFile(String fileName) throws FeatureStoreException {
    HopsworksClient hopsworksClient = getInstance();

    // Create a File object
    File file = Paths.get(
        System.getProperty("java.io.tmpdir"),
        hopsworksClient.getProject().getProjectName(),
        fileName).toFile();

    // Ensure parent directories exist
    File parentDir = file.getParentFile();
    if (parentDir != null && !parentDir.exists()) {
      parentDir.mkdirs();
    }

    return file;
  }

  @Getter
  @Setter
  protected HopsworksHttpClient hopsworksHttpClient;

  @Getter
  protected ObjectMapper objectMapper;

  @VisibleForTesting
  public HopsworksClient(HopsworksHttpClient hopsworksHttpClient, String host) {
    this.objectMapper = new ObjectMapper();
    this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    this.objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
    this.objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

    this.hopsworksHttpClient = hopsworksHttpClient;
    this.host = host;
  }

  @AllArgsConstructor
  @NoArgsConstructor
  protected static class HopsworksErrorClass {
    @Getter
    @Setter
    private Integer errorCode;
    @Getter
    @Setter
    private String usrMsg;
    @Getter
    @Setter
    private String devMsg;

    @Override
    public String toString() {
      return "errorCode=" + errorCode + ", usrMsg='" + usrMsg + '\'' + ", devMsg='" + devMsg + '\'';
    }
  }

  protected static class BaseHandler<T> implements ResponseHandler<T> {

    private Class<T> cls;
    private ObjectMapper objectMapper;

    public BaseHandler(Class<T> cls, ObjectMapper objectMapper) {
      this.cls = cls;
      this.objectMapper = objectMapper;
    }

    @Override
    public T handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
      String responseJson = EntityUtils.toString(response.getEntity(), Charset.defaultCharset());
      if (response.getStatusLine().getStatusCode() / 100 == 2) {
        return objectMapper.readValue(responseJson, cls);
      } else {
        HopsworksErrorClass error = objectMapper.readValue(responseJson, HopsworksErrorClass.class);
        LOGGER.info("Request error: " + response.getStatusLine().getStatusCode() + " " + error);
        throw new ClientProtocolException("Request error: " + response.getStatusLine().getStatusCode() + " " + error);
      }
    }
  }

  public <T> T handleRequest(HttpRequest request, ResponseHandler<T> responseHandler)
      throws IOException, FeatureStoreException {
    return hopsworksHttpClient.handleRequest(request, responseHandler);
  }

  public <T> T handleRequest(HttpRequest request, Class<T> cls) throws IOException, FeatureStoreException {
    return hopsworksHttpClient.handleRequest(request, new BaseHandler<>(cls, objectMapper));
  }

  public <T> T handleRequest(HttpRequest request) throws IOException, FeatureStoreException {
    return hopsworksHttpClient.handleRequest(request, null);
  }

  public StringEntity buildStringEntity(Object object) throws IOException {
    String jsonStrRepresentation = objectMapper.writeValueAsString(object);
    LOGGER.debug("Request content: " + jsonStrRepresentation);
    return new StringEntity(jsonStrRepresentation, ContentType.APPLICATION_JSON);
  }
}
