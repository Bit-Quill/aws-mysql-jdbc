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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.NullLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

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
  @Mock Future<?> taskB;
  @Mock HostInfo info;
  @Mock PropertySet propertySet;
  @Mock IMonitor monitorA;
  @Mock IMonitor monitorB;

  private final Log logger = new NullLogger("MultiThreadedDefaultMonitorServiceTest");

  private final AtomicInteger counter = new AtomicInteger(0);
  private final AtomicInteger concurrentCounter = new AtomicInteger(0);

  private static final int FAILURE_DETECTION_TIME = 10;
  private static final int FAILURE_DETECTION_INTERVAL = 100;
  private static final int FAILURE_DETECTION_COUNT = 3;

  private AutoCloseable closeable;
  private ArgumentCaptor<MonitorConnectionContext> captor;

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);

    when(monitorInitializer.createMonitor(Mockito.any(HostInfo.class), Mockito.any(PropertySet.class)))
        .thenReturn(monitorA, monitorB);
    when(executorServiceInitializer.createExecutorService()).thenReturn(service);
    doReturn(taskA, taskB).when(service).submit(Mockito.any(Monitor.class));

    captor = ArgumentCaptor.forClass(MonitorConnectionContext.class);

    doNothing().when(monitorA).startMonitoring(captor.capture());
    doNothing().when(monitorB).startMonitoring(captor.capture());
  }

  @AfterEach
  void cleanUp() throws Exception {
    counter.set(0);
    concurrentCounter.set(0);
    closeable.close();
  }

  /**
   * Create 2 connections to different nodes.
   */
  @RepeatedTest(1000)
  void test_1_multipleConnectionsToDifferentNodes() throws ExecutionException, InterruptedException, BrokenBarrierException {
    final int numConnections = 10;
    final List<Set<String>> nodeKeyList = generateNodeKeys(numConnections, true);
    final List<DefaultMonitorService> services = generateServices(numConnections);

    try {
      final List<Object> results = runMethodsAsync(
          numConnections,
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

      final List<MonitorConnectionContext> contexts = results.stream()
          .map(obj -> (MonitorConnectionContext) obj)
          .collect(Collectors.toList());

      final List<MonitorConnectionContext> capturedContexts = captor.getAllValues();

      assertEquals(numConnections, services.get(0).threadContainer.getMonitorMap().size());
      assertEquals(numConnections, capturedContexts.size());

      assertTrue((contexts.size() == capturedContexts.size())
          && contexts.containsAll(capturedContexts)
          && capturedContexts.containsAll(contexts));

      assertTrue(concurrentCounter.get() > 1);
      verify(monitorInitializer, times(numConnections)).createMonitor(eq(info), eq(propertySet));
    } finally {
      releaseResources(services);
    }
  }

  /**
   * Create 2 connections to one node.
   */
  @RepeatedTest(1000)
  void test_2_multipleConnectionsToOneNode() throws InterruptedException, BrokenBarrierException, ExecutionException {
    final int numConnections = 10;
    final List<Set<String>> nodeKeyList = generateNodeKeys(numConnections, false);
    final List<DefaultMonitorService> services = generateServices(numConnections);

    try {
      final List<Object> results = runMethodsAsync(
          numConnections,
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

      final List<MonitorConnectionContext> contexts = results.stream()
          .map(obj -> (MonitorConnectionContext) obj)
          .collect(Collectors.toList());
      final List<MonitorConnectionContext> capturedContexts = captor.getAllValues();

      assertEquals(1, services.get(0).threadContainer.getMonitorMap().size());
      assertEquals(numConnections, capturedContexts.size());
      assertTrue((contexts.size() == capturedContexts.size())
          && contexts.containsAll(capturedContexts)
          && capturedContexts.containsAll(contexts));

      assertTrue(concurrentCounter.get() > 1);
      verify(monitorInitializer).createMonitor(eq(info), eq(propertySet));
    } finally {
      releaseResources(services);
    }
  }

  private DefaultMonitorService createNewMonitorService() {
    return new DefaultMonitorService(
        monitorInitializer,
        executorServiceInitializer,
        logger);
  }

  private List<Object> runMethodsAsync(
      final int numConnections,
      final List<DefaultMonitorService> services,
      final List<Set<String>> nodeKeysList,
      final BiFunction<DefaultMonitorService, Set<String>, Object> method
  ) throws BrokenBarrierException, InterruptedException, ExecutionException {
    final String exceptionMessage = "Test thread interrupted due to an unexpected exception.";

    final CyclicBarrier gate = new CyclicBarrier(numConnections + 1);
    final List<CompletableFuture<Object>> threads = new ArrayList<>();

    for (int i = 0; i < numConnections; i++) {
      final DefaultMonitorService service = services.get(i);
      final Set<String> nodeKeys = nodeKeysList.get(i);

      threads.add(CompletableFuture.supplyAsync(() -> {
        try {
          gate.await();
        } catch (final InterruptedException | BrokenBarrierException e) {
          fail(exceptionMessage, e);
        }

        return method.apply(service, nodeKeys);
      }));
    }

    gate.await();

    final List<Object> contexts = new ArrayList<>();
    for (final CompletableFuture<Object> thread : threads) {
      contexts.add(thread.get());
    }

    return contexts;
  }

  private void releaseResources(final List<DefaultMonitorService> services) {
    for (final DefaultMonitorService defaultMonitorService : services) {
      defaultMonitorService.releaseResources();
    }
  }

  private List<Set<String>> generateNodeKeys(final int numConnections, final boolean diffNode) {
    final Set<String> singleNode = new HashSet<>(Collections.singletonList("node"));

    final List<Set<String>> nodeKeysList = new ArrayList<>();
    final Function<Integer, Set<String>> generateNodeKeysFunc = diffNode
        ? (i) -> new HashSet<>(Collections.singletonList(String.format("node%d", i)))
        : (i) -> singleNode;

    for (int i = 0; i < numConnections; i++) {
      nodeKeysList.add(generateNodeKeysFunc.apply(i));
    }

    return nodeKeysList;
  }

  private List<DefaultMonitorService> generateServices(final int numConnections) {
    final List<DefaultMonitorService> services = new ArrayList<>();
    for (int i = 0; i < numConnections; i++) {
      services.add(createNewMonitorService());
    }
    return services;
  }
}
