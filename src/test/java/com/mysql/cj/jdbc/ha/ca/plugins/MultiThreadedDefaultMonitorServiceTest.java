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

import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.NullLogger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Multi-threaded tests for {@link MultiThreadedDefaultMonitorServiceTest}.
 * Repeats each testcase multiple times.
 * Use a cyclic barrier to ensure threads start at the same time.
 */
class MultiThreadedDefaultMonitorServiceTest {
  @Mock IMonitorInitializer monitorInitializer;
  @Mock IExecutorServiceInitializer executorServiceInitializer;
  @Mock ExecutorService service;
  @Mock Future<?> taskA;
  @Mock HostInfo info;
  @Mock IMonitor monitor;

  private final Log logger = new NullLogger("MultiThreadedDefaultMonitorServiceTest");
  private final AtomicInteger counter = new AtomicInteger(0);
  private final AtomicInteger concurrentCounter = new AtomicInteger(0);

  private static final AtomicBoolean isTest1Concurrent = new AtomicBoolean(false);
  private static final AtomicBoolean isTest2Concurrent = new AtomicBoolean(false);
  private static final int FAILURE_DETECTION_TIME = 10;
  private static final int FAILURE_DETECTION_INTERVAL = 100;
  private static final int FAILURE_DETECTION_COUNT = 3;

  private PropertySet propertySet;
  private AutoCloseable closeable;
  private ArgumentCaptor<MonitorConnectionContext> captor;

  @BeforeEach
  void init() {
    propertySet = new DefaultPropertySet();
    closeable = MockitoAnnotations.openMocks(this);
    captor = ArgumentCaptor.forClass(MonitorConnectionContext.class);

    when(monitorInitializer.createMonitor(
        any(HostInfo.class),
        any(PropertySet.class),
        any(IMonitorService.class)))
        .thenReturn(monitor);
    when(executorServiceInitializer.createExecutorService()).thenReturn(service);
    doReturn(taskA).when(service).submit(any(Monitor.class));
    doNothing().when(monitor).startMonitoring(captor.capture());
  }

  @AfterEach
  void cleanUp() throws Exception {
    counter.set(0);
    concurrentCounter.set(0);
    closeable.close();
  }

  @AfterAll
  static void checkConcurrency() {
    assertTrue(isTest1Concurrent.get());
    assertTrue(isTest2Concurrent.get());
  }

  @RepeatedTest(1000)
  void test_1_multipleConnectionsToDifferentNodes()
      throws ExecutionException, InterruptedException {
    final int numConnections = 10;
    final List<Set<String>> nodeKeyList = generateNodeKeys(numConnections, true);
    final List<DefaultMonitorService> services = generateServices(numConnections);

    try {
      final List<MonitorConnectionContext> contexts = runStartMonitor(
          numConnections,
          services,
          nodeKeyList);

      final List<MonitorConnectionContext> capturedContexts = captor.getAllValues();

      assertEquals(numConnections, services.get(0).threadContainer.getMonitorMap().size());
      assertEquals(numConnections, capturedContexts.size());

      assertTrue((contexts.size() == capturedContexts.size())
          && contexts.containsAll(capturedContexts)
          && capturedContexts.containsAll(contexts));
      verify(monitorInitializer, times(numConnections)).createMonitor(eq(info), eq(propertySet), any(IMonitorService.class));

      if (concurrentCounter.get() != 0) {
        isTest1Concurrent.getAndSet(true);
      }
    } finally {
      releaseResources(services);
    }
  }

  @RepeatedTest(1000)
  void test_2_multipleConnectionsToOneNode() throws InterruptedException, ExecutionException {
    final int numConnections = 10;
    final List<Set<String>> nodeKeyList = generateNodeKeys(numConnections, false);
    final List<DefaultMonitorService> services = generateServices(numConnections);

    try {
      final List<MonitorConnectionContext> contexts = runStartMonitor(
          numConnections,
          services,
          nodeKeyList);

      final List<MonitorConnectionContext> capturedContexts = captor.getAllValues();

      assertEquals(1, services.get(0).threadContainer.getMonitorMap().size());
      assertEquals(numConnections, capturedContexts.size());

      assertTrue((contexts.size() == capturedContexts.size())
          && contexts.containsAll(capturedContexts)
          && capturedContexts.containsAll(contexts));

      verify(monitorInitializer).createMonitor(eq(info), eq(propertySet), any(IMonitorService.class));

      if (concurrentCounter.get() != 0) {
        isTest2Concurrent.getAndSet(true);
      }
    } finally {
      releaseResources(services);
    }
  }

  /**
   * Run {@link DefaultMonitorService#startMonitoring(Set, HostInfo, PropertySet, int, int, int)} multiple times.
   *
   * @param runs        The number of times to run the methods.
   * @param services    The {@link DefaultMonitorService} used to run each method.
   * @param nodeKeyList The sets of node keys for each service.
   * @return The {@link MonitorConnectionContext} returned by
   * {@link DefaultMonitorService#startMonitoring(Set, HostInfo, PropertySet, int, int, int)}.
   * @throws InterruptedException if a thread has been interrupted.
   * @throws ExecutionException   if an exception occurred within a thread.
   */
  private List<MonitorConnectionContext> runStartMonitor(
      final int runs,
      final List<DefaultMonitorService> services,
      final List<Set<String>> nodeKeyList)
      throws ExecutionException, InterruptedException {
    final List<Object> results = runMethodAsync(
        runs,
        services,
        nodeKeyList,
        (service, nodes) -> {
          final int val = counter.getAndIncrement();
          if (val != 0) {
            concurrentCounter.getAndIncrement();
          }

          final MonitorConnectionContext context = service
              .startMonitoring(
                  nodes,
                  info,
                  propertySet,
                  FAILURE_DETECTION_TIME,
                  FAILURE_DETECTION_INTERVAL,
                  FAILURE_DETECTION_COUNT);

          counter.getAndDecrement();
          return context;
        }
    );

    return results.stream()
        .map(obj -> (MonitorConnectionContext) obj)
        .collect(Collectors.toList());
  }

  /**
   * Run a {@link DefaultMonitorService} method concurrently in multiple threads.
   * A {@link CountDownLatch} is used to ensure all threads start at the same time.
   *
   * @param numThreads   The number of threads to create.
   * @param services     The services to run in each thread.
   * @param nodeKeysList The set of nodes assigned to each service.
   * @param method       A method in {@link DefaultMonitorService} to run in multiple threads.
   * @return the results from executing the method.
   * @throws InterruptedException if a thread has been interrupted.
   * @throws ExecutionException   if an exception occurred within a thread.
   */
  private List<Object> runMethodAsync(
      final int numThreads,
      final List<DefaultMonitorService> services,
      final List<Set<String>> nodeKeysList,
      final BiFunction<DefaultMonitorService, Set<String>, Object> method
  ) throws InterruptedException, ExecutionException {
    final String exceptionMessage = "Test thread interrupted due to an unexpected exception.";

    final CountDownLatch latch = new CountDownLatch(1);
    final List<CompletableFuture<Object>> threads = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      final DefaultMonitorService service = services.get(i);
      final Set<String> nodeKeys = nodeKeysList.get(i);

      threads.add(CompletableFuture.supplyAsync(() -> {
        try {
          // Wait until each thread is ready to start running.
          latch.await();
        } catch (final InterruptedException e) {
          fail(exceptionMessage, e);
        }

        // Execute the method.
        return method.apply(service, nodeKeys);
      }));
    }

    // Start all threads.
    latch.countDown();

    final List<Object> contexts = new ArrayList<>();
    for (final CompletableFuture<Object> thread : threads) {
      contexts.add(thread.get());
    }

    return contexts;
  }

  /**
   * Generate multiple sets of node keys pointing to either different nodes or the same node.
   *
   * @param numNodeKeys The amount of sets to create.
   * @param diffNode    Whether the node keys refer to different Aurora cluster nodes.
   * @return the sets of node keys.
   */
  private List<Set<String>> generateNodeKeys(final int numNodeKeys, final boolean diffNode) {
    final Set<String> singleNode = new HashSet<>(Collections.singletonList("node"));

    final List<Set<String>> nodeKeysList = new ArrayList<>();
    final Function<Integer, Set<String>> generateNodeKeysFunc = diffNode
        ? (i) -> new HashSet<>(Collections.singletonList(String.format("node%d", i)))
        : (i) -> singleNode;

    for (int i = 0; i < numNodeKeys; i++) {
      nodeKeysList.add(generateNodeKeysFunc.apply(i));
    }

    return nodeKeysList;
  }

  /**
   * Create multiple {@link DefaultMonitorService} objects.
   *
   * @param numServices The number of monitor services to create.
   * @return a list of monitor services.
   */
  private List<DefaultMonitorService> generateServices(final int numServices) {
    final List<DefaultMonitorService> services = new ArrayList<>();
    for (int i = 0; i < numServices; i++) {
      services.add(new DefaultMonitorService(
          monitorInitializer,
          executorServiceInitializer,
          logger));
    }
    return services;
  }

  /**
   * Release any resources used by the given services.
   *
   * @param services The {@link DefaultMonitorService} services to clean.
   */
  private void releaseResources(final List<DefaultMonitorService> services) {
    for (final DefaultMonitorService defaultMonitorService : services) {
      defaultMonitorService.releaseResources();
    }
  }
}
