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

import com.mysql.cj.conf.BooleanProperty;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.IntegerProperty;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.ha.ca.ConnectionProvider;
import com.mysql.cj.log.Log;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

class MonitorTest {

  @Mock
  ConnectionProvider connectionProvider;
  @Mock
  ConnectionImpl connection;
  @Mock
  HostInfo hostInfo;
  @Mock
  PropertySet propertySet;
  @Mock
  Log log;
  @Mock
  MonitorConnectionContext contextWithShortInterval;
  @Mock
  MonitorConnectionContext contextWithLongInterval;
  @Mock
  BooleanProperty booleanProperty;
  @Mock
  IntegerProperty intProperty;
  @Mock
  IExecutorServiceInitializer executorServiceInitializer;
  @Mock
  ExecutorService executorService;
  @Mock
  Future<?> futureResult;


  private static final int SHORT_INTERVAL_MILLIS = 30;
  private static final int SHORT_INTERVAL_SECONDS = SHORT_INTERVAL_MILLIS / 1000;
  private static final int LONG_INTERVAL_MILLIS = 300;
  private static final int MONITOR_TIMEOUT_MILLIS = 3000;

  private AutoCloseable closeable;
  private DefaultMonitorService monitorService;
  private Monitor monitor;

  @BeforeEach
  void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);

    Mockito
        .when(contextWithShortInterval.getFailureDetectionIntervalMillis())
        .thenReturn(SHORT_INTERVAL_MILLIS);
    Mockito
        .when(contextWithLongInterval.getFailureDetectionIntervalMillis())
        .thenReturn(LONG_INTERVAL_MILLIS);
    Mockito
        .when(propertySet.getBooleanProperty(Mockito.any(PropertyKey.class)))
        .thenReturn(booleanProperty);
    Mockito
        .when(booleanProperty.getStringValue())
        .thenReturn(Boolean.TRUE.toString());
    Mockito
        .when(propertySet.getIntegerProperty(Mockito.any(PropertyKey.class)))
        .thenReturn(intProperty);
    Mockito
        .when(intProperty.getValue())
        .thenReturn(SHORT_INTERVAL_MILLIS);

    // Set-up initial Monitor Map Container
    Mockito
        .when(propertySet.getIntegerProperty(Mockito.any(PropertyKey.class)))
        .thenReturn(intProperty);
    Mockito
        .when(intProperty.getValue())
        .thenReturn(MONITOR_TIMEOUT_MILLIS);

    // Set-up initial Monitor Map Container
    Mockito
        .when(connectionProvider.connect(Mockito.any(HostInfo.class)))
        .thenReturn(connection);
    Mockito
        .when(executorServiceInitializer.createExecutorService())
        .thenReturn(executorService);
    MonitorThreadContainer.getInstance(executorServiceInitializer);

    monitorService = new DefaultMonitorService(null);
    monitor = Mockito.spy(new Monitor(connectionProvider, hostInfo, propertySet,
        propertySet.getIntegerProperty(PropertyKey.monitorDisposeTime).getValue(), log));
    monitor.setService(monitorService);
  }

  @AfterEach
  void cleanUp() throws Exception {
    MonitorThreadContainer.releaseInstance();
    closeable.close();
  }

  @Test
  void test_1_startMonitoringWithDifferentContexts() {
    monitor.startMonitoring(contextWithShortInterval);
    monitor.startMonitoring(contextWithLongInterval);

    Assertions.assertEquals(
        SHORT_INTERVAL_MILLIS,
        monitor.getConnectionCheckIntervalMillis());
    Mockito
        .verify(contextWithShortInterval)
        .setStartMonitorTime(Mockito.anyLong());
    Mockito
        .verify(contextWithLongInterval)
        .setStartMonitorTime(Mockito.anyLong());
  }

  @Test
  void test_2_stopMonitoringWithContextRemaining() {
    monitor.startMonitoring(contextWithShortInterval);
    monitor.startMonitoring(contextWithLongInterval);

    monitor.stopMonitoring(contextWithShortInterval);
    Assertions.assertEquals(
        LONG_INTERVAL_MILLIS,
        monitor.getConnectionCheckIntervalMillis());
  }

  @Test
  void test_3_stopMonitoringWithNoMatchingContexts() {
    Assertions.assertDoesNotThrow(() -> monitor.stopMonitoring(contextWithLongInterval));
    Assertions.assertEquals(
        0,
        monitor.getConnectionCheckIntervalMillis());

    monitor.startMonitoring(contextWithShortInterval);
    Assertions.assertDoesNotThrow(() -> monitor.stopMonitoring(contextWithLongInterval));
    Assertions.assertEquals(
        SHORT_INTERVAL_MILLIS,
        monitor.getConnectionCheckIntervalMillis());
  }

  @Test
  void test_4_stopMonitoringTwiceWithSameContext() {
    monitor.startMonitoring(contextWithLongInterval);
    Assertions.assertDoesNotThrow(() -> monitor.stopMonitoring(contextWithLongInterval));
    Assertions.assertDoesNotThrow(() -> monitor.stopMonitoring(contextWithLongInterval));
    Assertions.assertEquals(
        0,
        monitor.getConnectionCheckIntervalMillis());
  }

  @Test
  void test_5_isConnectionHealthyWithNoExistingConnection() throws SQLException {
    final Monitor.ConnectionStatus status = monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);

    Mockito.verify(connectionProvider).connect(Mockito.any(HostInfo.class));
    Assertions.assertTrue(status.isValid);
    Assertions.assertEquals(0, status.elapsedTime);
  }

  @Test
  void test_6_isConnectionHealthyWithExistingConnection() throws SQLException {
    Mockito
        .when(connection.isValid(Mockito.eq(SHORT_INTERVAL_SECONDS)))
        .thenReturn(Boolean.TRUE, Boolean.FALSE);
    Mockito
        .when(connection.isClosed())
        .thenReturn(Boolean.FALSE);

    // Start up a monitoring connection.
    monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);

    final Monitor.ConnectionStatus status1 = monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);
    Assertions.assertTrue(status1.isValid);

    final Monitor.ConnectionStatus status2 = monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);
    Assertions.assertFalse(status2.isValid);

    Mockito.verify(connection, Mockito.times(2)).isValid(Mockito.anyInt());
  }

  @Test
  void test_7_isConnectionHealthyWithSQLException() throws SQLException {
    Mockito
        .when(connection.isValid(Mockito.anyInt()))
        .thenThrow(new SQLException());
    Mockito
        .when(connection.isClosed())
        .thenReturn(Boolean.FALSE);

    // Start up a monitoring connection.
    monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);

    Assertions.assertDoesNotThrow(() -> {
      final Monitor.ConnectionStatus status = monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);
      Assertions.assertFalse(status.isValid);
      Assertions.assertEquals(0, status.elapsedTime);
    });

    Mockito.verify(log).logTrace(Mockito.anyString(), Mockito.any(SQLException.class));
  }

  @RepeatedTest(1000)
  void test_8_runWithoutContext() {
    Mockito.doReturn((long) SHORT_INTERVAL_MILLIS)
        .when(monitor).getCurrentTimeMillis();

    // Put monitor into Container Map
    final Map<String, IMonitor> monitorMap = MonitorThreadContainer.getInstance().getMonitorMap();
    final Map<IMonitor, Future<?>> taskMap = MonitorThreadContainer.getInstance().getTasksMap();
    final String nodeKey = "monitorA";
    monitorMap.put(nodeKey, monitor);
    taskMap.put(monitor, futureResult);

    // Run monitor without contexts
    monitor.run();

    // After running with empty context, monitor should be out of the map
    Assertions.assertNull(monitorMap.get(nodeKey));
    Assertions.assertNull(taskMap.get(monitor));
  }

  @RepeatedTest(1000)
  void test_9_runWithContext() throws InterruptedException {
    // Put monitor into Container Map
    final Map<String, IMonitor> monitorMap = MonitorThreadContainer.getInstance().getMonitorMap();
    final Map<IMonitor, Future<?>> taskMap = MonitorThreadContainer.getInstance().getTasksMap();
    final String nodeKey = "monitorA";
    monitorMap.put(nodeKey, monitor);
    taskMap.put(monitor, futureResult);

    // Put context
    monitor.startMonitoring(contextWithShortInterval);
    // Set timer to remove
    Thread thread = new Thread(() -> {
      try {
        Thread.sleep(SHORT_INTERVAL_MILLIS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      finally {
        monitor.stopMonitoring(contextWithShortInterval);
      }
    });
    thread.start();

    // Run monitor, context removed after timer
    monitor.run();

    // After running monitor should be out of the map
    Assertions.assertNull(monitorMap.get(nodeKey));
    Assertions.assertNull(taskMap.get(monitor));
  }
}
