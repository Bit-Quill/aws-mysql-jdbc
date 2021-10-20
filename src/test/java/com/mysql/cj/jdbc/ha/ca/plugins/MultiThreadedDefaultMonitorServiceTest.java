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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Multi-threaded tests for {@link MultiThreadedDefaultMonitorServiceTest}.
 * Repeats each testcase multiple times.
 * Use a cyclic barrier to ensure threads start at the same time.
 */
class MultiThreadedDefaultMonitorServiceTest {

  private static class ExecutionTime {
    long startTime;
    long endTime;

    void setTime(long start, long end) {
      this.startTime = start;
      this.endTime = end;
    }

    boolean isOverlapped(ExecutionTime time) {
      return (Math.max(this.startTime, time.startTime) - Math.min(this.endTime, time.endTime)) < 0;
    }
  }

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
    closeable.close();
    DefaultMonitorService.MONITOR_MAP.clear();
    DefaultMonitorService.TASKS_MAP.values().forEach(val -> val.cancel(true));
    DefaultMonitorService.TASKS_MAP.clear();
  }

  /**
   * Create 2 connections to different nodes.
   */
  @RepeatedTest(1000)
  void test_1_multipleConnectionsToDifferentNodes() throws ExecutionException, InterruptedException, BrokenBarrierException {
    final List<Object> results = runMethodsAsync(
        (serviceA) -> serviceA.startMonitoring(
            "node1",
            info,
            propertySet,
            FAILURE_DETECTION_TIME,
            FAILURE_DETECTION_INTERVAL,
            FAILURE_DETECTION_COUNT),
        (serviceB) -> serviceB.startMonitoring(
            "node2",
            info,
            propertySet,
            FAILURE_DETECTION_TIME,
            FAILURE_DETECTION_INTERVAL,
            FAILURE_DETECTION_COUNT)
    );

    List<MonitorConnectionContext> contexts = results.stream()
        .map(obj -> (MonitorConnectionContext) obj)
        .collect(Collectors.toList());

    List<MonitorConnectionContext> capturedContexts = captor.getAllValues();

    assertEquals(2, DefaultMonitorService.MONITOR_MAP.size());
    assertEquals(2, capturedContexts.size());
    assertTrue((contexts.size() == capturedContexts.size())
        && contexts.containsAll(capturedContexts)
        && capturedContexts.containsAll(contexts));

    verify(monitorInitializer, times(2)).createMonitor(eq(info), eq(propertySet));
  }

  /**
   * Create 2 connections to one node.
   */
  @RepeatedTest(1)
  void test_2_multipleConnectionsToOneNode() throws ExecutionException, InterruptedException, BrokenBarrierException {
    final List<Object> results = runMethodsAsync(
        (serviceA) -> serviceA.startMonitoring(
            "node",
            info,
            propertySet,
            FAILURE_DETECTION_TIME,
            FAILURE_DETECTION_INTERVAL,
            FAILURE_DETECTION_COUNT),
        (serviceB) -> serviceB.startMonitoring(
            "node",
            info,
            propertySet,
            FAILURE_DETECTION_TIME,
            FAILURE_DETECTION_INTERVAL,
            FAILURE_DETECTION_COUNT)
    );

    List<MonitorConnectionContext> contexts = results.stream()
        .map(obj -> (MonitorConnectionContext) obj)
        .collect(Collectors.toList());
    List<MonitorConnectionContext> capturedContexts = captor.getAllValues();

    assertEquals(1, DefaultMonitorService.MONITOR_MAP.size());
    assertEquals(2, capturedContexts.size());
    assertTrue((contexts.size() == capturedContexts.size())
        && contexts.containsAll(capturedContexts)
        && capturedContexts.containsAll(contexts));

    verify(monitorInitializer).createMonitor(eq(info), eq(propertySet));
  }

  private DefaultMonitorService createNewMonitorService() {
    return new DefaultMonitorService(
        monitorInitializer,
        executorServiceInitializer,
        logger);
  }

  private List<Object> runMethodsAsync(
      final Function<DefaultMonitorService, Object> methodA,
      final Function<DefaultMonitorService, Object> methodB
  ) throws BrokenBarrierException, InterruptedException, ExecutionException {
    final String exceptionMessage = "Test thread interrupted due to an unexpected exception.";
    final ExecutionTime threadATime = new ExecutionTime();
    final ExecutionTime threadBTime = new ExecutionTime();

    final CyclicBarrier gate = new CyclicBarrier(3);
    final DefaultMonitorService serviceA = createNewMonitorService();
    final DefaultMonitorService serviceB = createNewMonitorService();

    final CompletableFuture<Object> threadA = CompletableFuture.supplyAsync(() -> {
      try {
        gate.await();
      } catch (final InterruptedException | BrokenBarrierException e) {
        fail(exceptionMessage, e);
      }

      final long startTime = System.nanoTime();
      final Object result = methodA.apply(serviceA);
      final long endTime = System.nanoTime();
      threadATime.setTime(startTime, endTime);
      return result;
    });

    final CompletableFuture<Object> threadB = CompletableFuture.supplyAsync(() -> {
      try {
        gate.await();
      } catch (final InterruptedException | BrokenBarrierException e) {
        fail(exceptionMessage, e);
      }

      final long startTime = System.nanoTime();
      final Object result = methodB.apply(serviceB);
      final long endTime = System.nanoTime();
      threadBTime.setTime(startTime, endTime);
      return result;
    });

    gate.await();

    final List<Object> contexts = new ArrayList<>();
    contexts.add(threadA.get());
    contexts.add(threadB.get());

    Assertions.assertTrue(threadATime.isOverlapped(threadBTime));

    return contexts;
  }
}
