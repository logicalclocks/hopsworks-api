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

package com.logicalclocks.hsfs.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.OnlineIngestion;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OnlineIngestionApi {

  private static final Logger LOGGER = LoggerFactory.getLogger(OnlineIngestionApi.class);

  public static final String ONLINE_INGESTION_PATH = "/featuregroups/{fgId}/online_ingestion{?queryParameters}";

  public OnlineIngestion createOnlineIngestion(FeatureGroupBase featureGroup, OnlineIngestion onlineIngestion)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + ONLINE_INGESTION_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", hopsworksClient.getProject().getProjectId())
        .set("fsId", featureGroup.getFeatureStore().getId())
        .set("fgId", featureGroup.getId())
        .expand();

    HttpPost postRequest = new HttpPost(uri);
    postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    postRequest.setEntity(hopsworksClient.buildStringEntity(onlineIngestion));

    LOGGER.info("Sending metadata request: " + uri);

    return hopsworksClient.handleRequest(postRequest, OnlineIngestion.class);
  }

  public OnlineIngestion getOnlineIngestion(FeatureGroupBase featureGroup, String queryParameters)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + ONLINE_INGESTION_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", hopsworksClient.getProject().getProjectId())
        .set("fsId", featureGroup.getFeatureStore().getId())
        .set("fgId", featureGroup.getId())
        .set("queryParameters", queryParameters)
        .expand();

    LOGGER.info("Sending metadata request: " + uri);

    return hopsworksClient.handleRequest(new HttpGet(uri), OnlineIngestion.class);
  }
}
