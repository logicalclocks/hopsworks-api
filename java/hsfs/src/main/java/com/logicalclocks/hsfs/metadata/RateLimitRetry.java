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

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Resends a request the rate limiter refused after its Retry-After, for up to 60 seconds in total.
 * Once the next wait would pass that budget the last {@link RateLimitedException} is thrown.
 */
class RateLimitRetry {

  private static final Logger LOGGER = LoggerFactory.getLogger(RateLimitRetry.class);
  static final long BUDGET_MILLIS = TimeUnit.SECONDS.toMillis(60);
  static final RateLimitRetry DEFAULT =
      new RateLimitRetry(() -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime()), Thread::sleep);

  interface Attempt<T, E extends Exception> {
    T call() throws IOException, E;
  }

  interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  private final LongSupplier clockMillis;
  private final Sleeper sleeper;

  RateLimitRetry(LongSupplier clockMillis, Sleeper sleeper) {
    this.clockMillis = clockMillis;
    this.sleeper = sleeper;
  }

  <T, E extends Exception> T execute(HttpRequest request, Attempt<T, E> attempt) throws IOException, E {
    boolean replayable = isReplayable(request);
    long deadline = clockMillis.getAsLong() + BUDGET_MILLIS;
    while (true) {
      try {
        return attempt.call();
      } catch (RateLimitedException e) {
        long waitMillis = TimeUnit.SECONDS.toMillis(e.getRetryAfterSeconds());
        if (!replayable || waitMillis > deadline - clockMillis.getAsLong()) {
          throw e;
        }
        LOGGER.info("Rate limited by Hopsworks, retrying in " + e.getRetryAfterSeconds() + " s: " + request);
        try {
          sleeper.sleep(waitMillis);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException("Interrupted while waiting to retry a rate limited request");
        }
        if (clockMillis.getAsLong() > deadline) {
          throw e;
        }
      }
    }
  }

  private static boolean isReplayable(HttpRequest request) {
    if (!(request instanceof HttpEntityEnclosingRequest)) {
      return true;
    }
    HttpEntity entity = ((HttpEntityEnclosingRequest) request).getEntity();
    return entity == null || entity.isRepeatable();
  }
}
