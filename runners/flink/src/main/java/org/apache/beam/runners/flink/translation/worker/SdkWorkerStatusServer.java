/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.flink.translation.worker;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import java.io.IOException;
import java.io.OutputStream;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.fnexecution.status.BeamWorkerStatusGrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Servlet dedicated to provide live status info retrieved from SDK Harness. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SdkWorkerStatusServer {
  private static final Logger LOG = LoggerFactory.getLogger(SdkWorkerStatusServer.class);
  private static final int BASE_PORT = 5100;
  private static final AtomicBoolean started = new AtomicBoolean(false);

  public static void create() {
    if (started.compareAndSet(false, true)) {
      AtomicInteger attempt = new AtomicInteger(0);

      RetryPolicy<HttpServer> retryPolicy =
          RetryPolicy.<HttpServer>builder()
              .handle(BindException.class, IOException.class)
              .withBackoff(
                  Duration.ofMillis(500), Duration.ofSeconds(10), 2.0) // exponential backoff
              .withMaxAttempts(5)
              .onFailedAttempt(
                  e -> {
                    int port = BASE_PORT + attempt.get() - 1;
                    LOG.warn(
                        "Failed to bind to port "
                            + port
                            + ": "
                            + e.getLastException().getMessage());
                  })
              .onRetriesExceeded(e -> LOG.warn("Exceeded max retries. Giving up."))
              .build();

      try {
        HttpServer server =
            Failsafe.with(retryPolicy)
                .get(
                    () -> {
                      int port = BASE_PORT + attempt.getAndIncrement();
                      HttpServer httpServer = HttpServer.create(new InetSocketAddress(port), 0);
                      // Define a context (path) and handler
                      httpServer.createContext("/workerstatus", new WorkerStatusHandler());
                      // Start the server
                      httpServer.setExecutor(null); // creates a default executor
                      httpServer.start();
                      LOG.info(
                          "SdkWorkerStatusServer started at " + httpServer.getAddress().toString());
                      return httpServer;
                    });

        // Add shutdown hook
        Runtime.getRuntime()
            .addShutdownHook(
                new Thread(
                    () -> {
                      LOG.info("Shutting down SdkWorkerStatusServer...");
                      server.stop(0);
                    }));
      } catch (Exception e) {
        LOG.warn("Fail to start SdkWorkerStatusServer.");
      }
    }
  }

  // Simple handler
  static class WorkerStatusHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      BeamWorkerStatusGrpcService statusGrpcService = BeamWorkerStatusGrpcService.getInstance();
      Map<String, String> allStatuses =
          statusGrpcService.getAllWorkerStatuses(10, TimeUnit.SECONDS);
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, String> entry : allStatuses.entrySet()) {
        sb.append(entry.getKey());
        sb.append("\n");
        sb.append(entry.getValue());
        sb.append("\n");
      }

      String response = sb.toString();
      exchange.sendResponseHeaders(200, response.length());
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(response.getBytes(StandardCharsets.UTF_8));
      }
    }
  }
}
