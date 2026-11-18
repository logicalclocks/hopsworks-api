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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.Charset;

public class AuthorizationHandler<T> implements ResponseHandler<T> {

  // GenericErrorCode.TOO_MANY_REQUESTS from the backend's per-user rate limiter.
  static final int RATE_LIMITED_ERROR_CODE = 120015;
  // httpcore 4 has no constant for 429.
  static final int SC_TOO_MANY_REQUESTS = 429;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private ResponseHandler<T> originalResponseHandler;

  AuthorizationHandler(ResponseHandler<T> originalResponseHandler) {
    this.originalResponseHandler = originalResponseHandler;
  }

  AuthorizationHandler() {
  }

  @Override
  public T handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
    if (response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
      throw new UnauthorizedException();
    } else if (response.getStatusLine().getStatusCode() / 100 == 4) {
      String body = EntityUtils.toString(response.getEntity(), Charset.defaultCharset());
      String message = "Error: " + response.getStatusLine().getStatusCode() + body;
      Long retryAfterSeconds = rateLimitedWait(response, body);
      if (retryAfterSeconds != null) {
        throw new RateLimitedException(message, retryAfterSeconds);
      }
      throw new IOException(message);
    } else if (response.getStatusLine().getStatusCode() / 100 == 5) {
      throw new InternalException("Error: " + response.getStatusLine().getStatusCode()
          + EntityUtils.toString(response.getEntity(), Charset.defaultCharset()));
    }

    if (originalResponseHandler != null) {
      return originalResponseHandler.handleResponse(response);
    }
    return null;
  }

  /**
   * Seconds the rate limiter asks the client to wait, or null if the response is not a rate-limit refusal.
   */
  private static Long rateLimitedWait(HttpResponse response, String body) {
    if (response.getStatusLine().getStatusCode() != SC_TOO_MANY_REQUESTS) {
      return null;
    }
    Header retryAfter = response.getFirstHeader("Retry-After");
    if (retryAfter == null) {
      return null;
    }
    try {
      long seconds = Long.parseLong(retryAfter.getValue().trim());
      JsonNode error = OBJECT_MAPPER.readTree(body);
      if (seconds < 0 || error == null || error.path("errorCode").asInt() != RATE_LIMITED_ERROR_CODE) {
        return null;
      }
      return seconds;
    } catch (NumberFormatException | IOException e) {
      return null;
    }
  }
}
