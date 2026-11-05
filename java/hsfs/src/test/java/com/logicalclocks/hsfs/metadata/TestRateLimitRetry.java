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
package com.logicalclocks.hsfs.metadata;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TestRateLimitRetry {

  private final long[] clock = {1000L};
  private final List<Long> sleeps = new ArrayList<>();
  private final RateLimitRetry retry = new RateLimitRetry(() -> clock[0], millis -> {
    sleeps.add(millis);
    clock[0] += millis;
  });

  private static HttpResponse rateLimited(String retryAfter) throws IOException {
    HttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, AuthorizationHandler.SC_TOO_MANY_REQUESTS, "");
    response.setHeader("Retry-After", retryAfter);
    response.setEntity(new StringEntity("{\"errorCode\":" + AuthorizationHandler.RATE_LIMITED_ERROR_CODE + "}"));
    return response;
  }

  private static HttpResponse ok() {
    return new BasicHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "");
  }

  private String send(HttpRequest request, HttpResponse... responses) throws IOException {
    Iterator<HttpResponse> sent = Arrays.asList(responses).iterator();
    AuthorizationHandler<String> handler = new AuthorizationHandler<>(response -> "done");
    return retry.execute(request, () -> handler.handleResponse(sent.next()));
  }

  @Test
  public void testRateLimitedRequestWaitsAndRetries() throws IOException {
    Assertions.assertEquals("done", send(new HttpGet("/"), rateLimited("3"), ok()));
    Assertions.assertEquals(Arrays.asList(3000L), sleeps);
  }

  @Test
  public void testWaitBeyondBudgetThrows() throws IOException {
    Assertions.assertThrows(RateLimitedException.class,
        () -> send(new HttpGet("/"), rateLimited("30"), rateLimited("31"), ok()));
    Assertions.assertEquals(Arrays.asList(30000L), sleeps);
  }

  @Test
  public void testNonRepeatableEntityIsNotRetried() {
    HttpPost post = new HttpPost("/");
    post.setEntity(new InputStreamEntity(new ByteArrayInputStream(new byte[] {1})));
    Assertions.assertThrows(RateLimitedException.class, () -> send(post, rateLimited("1"), ok()));
    Assertions.assertTrue(sleeps.isEmpty());
  }

  @Test
  public void testRepeatableEntityIsRetried() throws IOException {
    HttpPost post = new HttpPost("/");
    post.setEntity(new StringEntity("{}"));
    Assertions.assertEquals("done", send(post, rateLimited("1"), ok()));
  }

  @Test
  public void testOtherTooManyRequestsIsNotRetried() throws IOException {
    HttpResponse response = rateLimited("1");
    response.setEntity(new StringEntity("{\"errorCode\":270999}"));
    IOException error = Assertions.assertThrows(IOException.class, () -> send(new HttpGet("/"), response, ok()));
    Assertions.assertFalse(error instanceof RateLimitedException);
    Assertions.assertTrue(sleeps.isEmpty());
  }
}
