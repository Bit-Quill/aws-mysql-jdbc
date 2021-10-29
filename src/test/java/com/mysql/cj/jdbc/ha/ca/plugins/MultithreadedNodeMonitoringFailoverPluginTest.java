/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package com.mysql.cj.jdbc.ha.ca.plugins;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInfo;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Multi-threaded tests for {@link NodeMonitoringFailoverPlugin#execute(String, Callable)}.
 */
public class MultithreadedNodeMonitoringFailoverPluginTest extends NodeMonitoringFailoverPluginBaseTest {
  private final AtomicInteger counter = new AtomicInteger(0);
  private final AtomicInteger concurrentCounter = new AtomicInteger(0);
  private static final int NUM_PLUGINS_PER_SERVICE = 2;

  /**
   * Keep track of whether each test has been executed concurrently at least once.
   */
  private static final Map<String, AtomicBoolean> CONCURRENT_TEST_MAP = new ConcurrentHashMap<>();

  private AutoCloseable closeable;

  @BeforeEach
  void init(TestInfo testInfo) throws Exception {
    closeable = MockitoAnnotations.openMocks(this);
    CONCURRENT_TEST_MAP.computeIfAbsent(
        testInfo.getDisplayName(),
        k -> new AtomicBoolean(false));

    initDefaultMockReturns();
  }

  @AfterEach
  void cleanUp(TestInfo testInfo) throws Exception {
    counter.set(0);

    if (concurrentCounter.get() > 0) {
      CONCURRENT_TEST_MAP.get(testInfo.getDisplayName()).getAndSet(true);
    }

    concurrentCounter.set(0);
    closeable.close();
  }

  /**
   * Ensure each test case was executed concurrently at least once.
   */
  @AfterAll
  static void assertConcurrency() {
    CONCURRENT_TEST_MAP.forEach((key, value) -> assertTrue(
        value.get(),
        String.format("Test '%s' was executed sequentially.", key)));
  }

  @RepeatedTest(value = 100, name = "execute successful")
  void test_1_executeSuccessful() throws ExecutionException, InterruptedException {
    when(context.isNodeUnhealthy()).thenReturn(false);
    final int numConnections = 10;
    final int wantedNumberOfInvocations = NUM_PLUGINS_PER_SERVICE * numConnections;
    final List<NodeMonitoringFailoverPlugin> plugins = initPlugins(numConnections);
    List<CompletableFuture<Object>> threads = runExecuteAsync(numConnections, plugins);

    for (final CompletableFuture<Object> thread : threads) {
      thread.get();
    }

    verify(monitorService, times(wantedNumberOfInvocations)).startMonitoring(
        anySet(),
        any(HostInfo.class),
        any(PropertySet.class),
        anyInt(),
        anyInt(),
        anyInt()
    );

    // Expect 2 times the number of connections.
    // StartMonitoring and StopMonitoring are called once in plugin and once more in next plugin.
    verify(monitorService, times(wantedNumberOfInvocations)).stopMonitoring(eq(context));
  }

  @RepeatedTest(value = 100, name = "execute with exception")
  void test_2_executeWithException() throws Exception {
    when(context.isNodeUnhealthy()).thenReturn(true);
    when(mockPlugin.execute(anyString(), eq(sqlFunction))).thenAnswer(invocation -> {
      // Sleep for a while to imitate a long query
      // and allow the monitoring thread time to check node status.
      Thread.sleep(60 * 1000);
      return "done";
    });
    final int numConnections = 10;
    final int wantedNumberOfInvocations = NUM_PLUGINS_PER_SERVICE * numConnections;

    final List<NodeMonitoringFailoverPlugin> plugins = initPlugins(numConnections);
    List<CompletableFuture<Object>> threads = runExecuteAsync(numConnections, plugins);

    final Throwable originalException = assertThrows(
        CompletionException.class,
        () -> CompletableFuture.allOf(threads.toArray(new CompletableFuture[0])).join())
        .getCause();
    assertTrue(originalException instanceof CJCommunicationsException);

    verify(monitorService, times(wantedNumberOfInvocations)).startMonitoring(
        anySet(),
        any(HostInfo.class),
        any(PropertySet.class),
        anyInt(),
        anyInt(),
        anyInt()
    );

    for (final CompletableFuture<Object> thread : threads) {
      assertTrue(thread.isCompletedExceptionally());
    }
    verify(monitorService, atLeast(wantedNumberOfInvocations - 1)).stopMonitoring(eq(context));
  }

  @RepeatedTest(value = 100, name = "execute with monitoring feature disabled")
  void test_3_executeWithMonitorDisabled() throws ExecutionException, InterruptedException {
    when(nativeFailureDetectionEnabledProperty.getValue()).thenReturn(false);
    final int numConnections = 10;

    final List<NodeMonitoringFailoverPlugin> plugins = initPlugins(numConnections);
    List<CompletableFuture<Object>> threads = runExecuteAsync(numConnections, plugins);

    for (final CompletableFuture<Object> thread : threads) {
      thread.get();
    }

    verify(monitorService, never()).startMonitoring(
        anySet(),
        any(HostInfo.class),
        any(PropertySet.class),
        anyInt(),
        anyInt(),
        anyInt()
    );

    verify(monitorService, never()).stopMonitoring(eq(context));
  }

  private List<NodeMonitoringFailoverPlugin> initPlugins(final int numPlugins) {
    final List<NodeMonitoringFailoverPlugin> plugins = new ArrayList<>();

    for (int i = 0; i < numPlugins; i++) {
      final NodeMonitoringFailoverPlugin nextPlugin = new NodeMonitoringFailoverPlugin(connection, propertySet, hostInfo, mockPlugin, logger, initializer);
      final NodeMonitoringFailoverPlugin plugin = new NodeMonitoringFailoverPlugin(connection, propertySet, hostInfo, nextPlugin, logger, initializer);
      plugins.add(plugin);
    }

    return plugins;
  }

  private List<CompletableFuture<Object>> runExecuteAsync(
      int numConnections,
      List<NodeMonitoringFailoverPlugin> plugins
  ) {
    final String exceptionMessage = "Test thread interrupted due to an unexpected exception.";
    final List<CompletableFuture<Object>> threads = new ArrayList<>();
    final CountDownLatch latch = new CountDownLatch(1);

    for (int i = 0; i < numConnections; i++) {
      final NodeMonitoringFailoverPlugin plugin = plugins.get(i);
      threads.add(CompletableFuture.supplyAsync(() -> {
        try {
          try {
            // Wait until each thread is ready to start running.
            latch.await();
          } catch (final InterruptedException e) {
            fail(exceptionMessage, e);
          }

          final int val = counter.getAndIncrement();
          if (val != 0) {
            concurrentCounter.getAndIncrement();
          }

          final Object result = plugin.execute(NodeMonitoringFailoverPluginBaseTest.MONITOR_METHOD_NAME, sqlFunction);
          counter.getAndDecrement();
          return result;
        } catch (Exception e) {
          if (e instanceof CJCommunicationsException) {
            throw new CompletionException(e);
          }
          fail("unexpected error", e);
          return null;
        }
      }));
    }

    latch.countDown();
    return threads;
  }
}
