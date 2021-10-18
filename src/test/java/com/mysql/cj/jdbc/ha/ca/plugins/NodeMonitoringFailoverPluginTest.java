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
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.NullLogger;
import org.jboss.invocation.InvocationException;
import org.jboss.util.NullArgumentException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

class NodeMonitoringFailoverPluginTest {

  @Mock
  private NodeMonitoringFailoverPlugin.IMonitorServiceInitializer monitorServiceInitializer;
  @Mock
  private PropertySet propertySet;
  @Mock
  private HostInfo hostInfo;
  @Mock
  private IFailoverPlugin mockPlugin;
  @Mock
  private Log logger;
  @Mock
  private IMonitorService monitorService;
  @Mock
  private Callable sqlFunc;
  @Mock
  private MonitorConnectionContext context;

  @Mock
  private RuntimeProperty<Boolean> nativeFailureDetectionEnabledProperty;
  @Mock
  private RuntimeProperty<Integer> failureDetectionTimeProperty;
  @Mock
  private RuntimeProperty<Integer> failureDetectionIntervalProperty;
  @Mock
  private RuntimeProperty<Integer> failureDetectionCountProperty;


  private static final String NODE = "node";
  private static final String NO_MONITOR_METHOD_NAME = "foo";
  private static final String MONITOR_METHOD_NAME = "executeQuery";

  private static final int FAILURE_DETECTION_TIME = 10;
  private static final int FAILURE_DETECTION_INTERVAL = 100;
  private static final int FAILURE_DETECTION_COUNT = 5;
  private static final ExecutionException EXECUTION_EXCEPTION = new ExecutionException(
      "exception",
      new InvocationException("exception", new Throwable()));

  private NodeMonitoringFailoverPlugin plugin;
  private AutoCloseable closeable;

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);
    plugin = new NodeMonitoringFailoverPlugin();

    initDefaultMockReturns();
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @ParameterizedTest
  @MethodSource("generateNullArguments")
  void test_1_initWithNullArguments(
      final PropertySet set,
      final HostInfo info,
      final IFailoverPlugin failoverPlugin,
      final Log log) {
    Assertions.assertThrows(
        NullArgumentException.class,
        () -> plugin.init(set, info, failoverPlugin, log));
  }

  @Test
  void test_2_initWithFailoverEnabled() {
    Mockito
        .when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);

    initializePlugin();

    Mockito.verify(monitorServiceInitializer).create(Mockito.eq(logger));
  }

  @Test
  void test_3_initWithFailoverDisabled() {
    Mockito
        .when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.FALSE);

    initializePlugin();
    Mockito.verify(monitorServiceInitializer, Mockito.never()).create(Mockito.any());
  }

  @Test
  void test_4_executeWithFailoverDisabled() {
    Mockito
        .when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.FALSE);
    initializePlugin();

    Assertions.assertDoesNotThrow(() -> {
      plugin.execute(MONITOR_METHOD_NAME, sqlFunc);
      Mockito
          .verify(mockPlugin)
          .execute(Mockito.eq(MONITOR_METHOD_NAME), Mockito.eq(sqlFunc));
    });
  }

  @Test
  void test_5_executeWithNoNeedToMonitor() {
    Mockito
        .when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);
    initializePlugin();

    Assertions.assertDoesNotThrow(() -> {
      plugin.execute(NO_MONITOR_METHOD_NAME, sqlFunc);
      Mockito
          .verify(mockPlugin)
          .execute(Mockito.eq(NO_MONITOR_METHOD_NAME), Mockito.eq(sqlFunc));
    });
  }

  @Test
  void test_6_executeThrowsExecutionException() {
    Mockito
        .when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);

    initializePlugin();

    Assertions.assertThrows(Exception.class, () -> {
      Mockito
          .when(mockPlugin.execute(Mockito.any(), Mockito.any()))
          .thenThrow(EXECUTION_EXCEPTION);

      plugin.execute(MONITOR_METHOD_NAME, sqlFunc);
    });

    Mockito.verify(monitorService).stopMonitoring(Mockito.eq(context));
    Mockito.verify(logger, Mockito.atLeastOnce()).logTrace(Mockito.anyString());
  }

  @Test
  void test_7_executeWithUnhealthyNode() {
    Mockito
        .when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);

    initializePlugin();

    Assertions.assertThrows(CJCommunicationsException.class, () -> {
      Mockito
          .when(context.isNodeUnhealthy())
          .thenReturn(Boolean.TRUE);

      Mockito
          .when(mockPlugin.execute(Mockito.any(), Mockito.any()))
          .thenAnswer(invocation -> {
            // Imitate running a long query;
            Thread.sleep(60 * 1000);
            return "done";
          });

      plugin.execute(MONITOR_METHOD_NAME, sqlFunc);
    });

    Mockito.verify(context).isNodeUnhealthy();
    Mockito.verify(monitorService).stopMonitoring(Mockito.eq(context));
    Mockito.verify(logger, Mockito.atLeastOnce()).logTrace(Mockito.anyString());
  }

  @Test
  void test_8_executeSuccessfulWithLongQuery() {
    final String expected = "foo";

    Mockito
        .when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);

    initializePlugin();

    Assertions.assertDoesNotThrow(() -> {
      Mockito
          .when(context.isNodeUnhealthy())
          .thenReturn(Boolean.FALSE);

      Mockito
          .when(mockPlugin.execute(Mockito.any(), Mockito.any()))
          .thenAnswer(invocation -> {
            // Imitate running a query for 10 seconds.
            Thread.sleep(10 * 1000);
            return expected;
          });

      final Object result = plugin.execute(MONITOR_METHOD_NAME, sqlFunc);
      Assertions.assertEquals(expected, result);
    });

    Mockito.verify(context, Mockito.atLeastOnce()).isNodeUnhealthy();
    Mockito.verify(monitorService).stopMonitoring(Mockito.eq(context));
    Mockito.verify(logger, Mockito.atLeastOnce()).logTrace(Mockito.anyString());
  }

  private static Stream<Arguments> generateNullArguments() {
    final PropertySet set = new DefaultPropertySet();
    final HostInfo info = new HostInfo();
    final IFailoverPlugin failoverPlugin = new DefaultFailoverPlugin();
    final Log log = new NullLogger("NodeMonitoringFailoverPluginTest");

    return Stream.of(
        Arguments.of(null, info, failoverPlugin, log),
        Arguments.of(set, null, failoverPlugin, log),
        Arguments.of(set, info, null, log),
        Arguments.of(set, info, failoverPlugin, null)
    );
  }

  private void initDefaultMockReturns() {
    Mockito
        .when(hostInfo.getHost())
        .thenReturn(NODE);
    Mockito
        .when(monitorServiceInitializer.create(Mockito.any()))
        .thenReturn(monitorService);
    Mockito
        .when(monitorService.startMonitoring(
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.anyInt()))
        .thenReturn(context);

    Mockito
        .when(propertySet.getBooleanProperty(Mockito.eq(PropertyKey.nativeFailureDetectionEnabled)))
        .thenReturn(nativeFailureDetectionEnabledProperty);
    Mockito
        .when(propertySet.getIntegerProperty(Mockito.eq(PropertyKey.failureDetectionTime)))
        .thenReturn(failureDetectionTimeProperty);
    Mockito
        .when(propertySet.getIntegerProperty(Mockito.eq(PropertyKey.failureDetectionInterval)))
        .thenReturn(failureDetectionIntervalProperty);
    Mockito
        .when(propertySet.getIntegerProperty(Mockito.eq(PropertyKey.failureDetectionCount)))
        .thenReturn(failureDetectionCountProperty);

    Mockito
        .when(failureDetectionTimeProperty.getValue())
        .thenReturn(FAILURE_DETECTION_TIME);
    Mockito
        .when(failureDetectionIntervalProperty.getValue())
        .thenReturn(FAILURE_DETECTION_INTERVAL);
    Mockito
        .when(failureDetectionCountProperty.getValue())
        .thenReturn(FAILURE_DETECTION_COUNT);
  }

  private void initializePlugin() {
    plugin.init(propertySet, hostInfo, mockPlugin, logger, monitorServiceInitializer);
  }
}
