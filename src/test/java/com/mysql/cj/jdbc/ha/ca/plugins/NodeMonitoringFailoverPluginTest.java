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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NodeMonitoringFailoverPluginTest {

  @Mock
  private NodeMonitoringFailoverPlugin.IMonitorServiceInitializer monitorServiceInitializer;
  @Mock
  private Connection connection;
  @Mock
  private Statement statement;
  @Mock
  private ResultSet resultSet;
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
  void init() throws SQLException {
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
      final Connection connection,
      final PropertySet set,
      final HostInfo info,
      final IFailoverPlugin failoverPlugin,
      final Log log) {
    Assertions.assertThrows(
        NullArgumentException.class,
        () -> plugin.init(connection, set, info, failoverPlugin, log));
  }

  @Test
  void test_2_initWithFailoverEnabled() {
    when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);

    initializePlugin();

    verify(monitorServiceInitializer).create(Mockito.eq(logger));
  }

  @Test
  void test_3_initWithFailoverDisabled() {
    when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.FALSE);

    initializePlugin();
    verify(monitorServiceInitializer, Mockito.never()).create(Mockito.any());
  }

  @Test
  void test_4_executeWithFailoverDisabled() throws Exception {
    when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.FALSE);
    initializePlugin();

    plugin.execute(MONITOR_METHOD_NAME, sqlFunc);
    verify(mockPlugin).execute(Mockito.eq(MONITOR_METHOD_NAME), Mockito.eq(sqlFunc));
  }

  @Test
  void test_5_executeWithNoNeedToMonitor() throws Exception {
    when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);
    initializePlugin();

    plugin.execute(NO_MONITOR_METHOD_NAME, sqlFunc);
    verify(mockPlugin)
        .execute(Mockito.eq(NO_MONITOR_METHOD_NAME), Mockito.eq(sqlFunc));
  }

  @Test
  void test_6_executeThrowsExecutionException() {
    when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);

    initializePlugin();

    Assertions.assertThrows(Exception.class, () -> {
      when(mockPlugin.execute(Mockito.any(), Mockito.any()))
          .thenThrow(EXECUTION_EXCEPTION);

      plugin.execute(MONITOR_METHOD_NAME, sqlFunc);
    });

    verify(monitorService).stopMonitoring(Mockito.eq(context));
  }

  @Test
  void test_7_executeWithUnhealthyNode() {
    when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);

    initializePlugin();

    Assertions.assertThrows(CJCommunicationsException.class, () -> {
      when(context.isNodeUnhealthy())
          .thenReturn(Boolean.TRUE);

      when(mockPlugin.execute(Mockito.any(), Mockito.any()))
          .thenAnswer(invocation -> {
            // Imitate running a long query;
            Thread.sleep(60 * 1000);
            return "done";
          });

      plugin.execute(MONITOR_METHOD_NAME, sqlFunc);
    });

    verify(context).isNodeUnhealthy();
    verify(monitorService).stopMonitoring(Mockito.eq(context));
  }

  @Test
  void test_8_executeSuccessfulWithLongQuery() throws Exception {
    final String expected = "foo";

    when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);
    when(context.isNodeUnhealthy())
        .thenReturn(Boolean.FALSE);
    when(mockPlugin.execute(Mockito.any(), Mockito.any()))
        .thenAnswer(invocation -> {
          // Imitate running a query for 10 seconds.
          Thread.sleep(10 * 1000);
          return expected;
        });

    initializePlugin();

    final Object result = plugin.execute(MONITOR_METHOD_NAME, sqlFunc);
    Assertions.assertEquals(expected, result);

    verify(context, Mockito.atLeastOnce()).isNodeUnhealthy();
    verify(monitorService).stopMonitoring(Mockito.eq(context));
  }

  /**
   * Generate different sets of method arguments where one argument is null to ensure
   * {@link NodeMonitoringFailoverPlugin#init(Connection, PropertySet, HostInfo, IFailoverPlugin, Log)}
   * can handle null arguments correctly.
   *
   * @return different sets of arguments.
   */
  private static Stream<Arguments> generateNullArguments() {
    final Connection connection = Mockito.mock(Connection.class);
    final PropertySet set = new DefaultPropertySet();
    final HostInfo info = new HostInfo();
    final IFailoverPlugin failoverPlugin = new DefaultFailoverPlugin();
    final Log log = new NullLogger("NodeMonitoringFailoverPluginTest");

    return Stream.of(
        Arguments.of(null, set, info, failoverPlugin, log),
        Arguments.of(connection, null, info, failoverPlugin, log),
        Arguments.of(connection, set, null, failoverPlugin, log),
        Arguments.of(connection, set, info, null, log),
        Arguments.of(connection, set, info, failoverPlugin, null)
    );
  }

  private void initDefaultMockReturns() throws SQLException {
    when(hostInfo.getHost())
        .thenReturn(NODE);
    when(monitorServiceInitializer.create(Mockito.any()))
        .thenReturn(monitorService);
    when(monitorService.startMonitoring(
        Mockito.anySet(),
        Mockito.any(HostInfo.class),
        Mockito.any(PropertySet.class),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.anyInt()))
        .thenReturn(context);

    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery(Mockito.anyString())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);
    when(hostInfo.getHost()).thenReturn("host");
    when(hostInfo.getHost()).thenReturn("port");

    when(propertySet.getBooleanProperty(Mockito.eq(PropertyKey.nativeFailureDetectionEnabled)))
        .thenReturn(nativeFailureDetectionEnabledProperty);
    when(propertySet.getIntegerProperty(Mockito.eq(PropertyKey.failureDetectionTime)))
        .thenReturn(failureDetectionTimeProperty);
    when(propertySet.getIntegerProperty(Mockito.eq(PropertyKey.failureDetectionInterval)))
        .thenReturn(failureDetectionIntervalProperty);
    when(propertySet.getIntegerProperty(Mockito.eq(PropertyKey.failureDetectionCount)))
        .thenReturn(failureDetectionCountProperty);

    when(failureDetectionTimeProperty.getValue())
        .thenReturn(FAILURE_DETECTION_TIME);
    when(failureDetectionIntervalProperty.getValue())
        .thenReturn(FAILURE_DETECTION_INTERVAL);
    when(failureDetectionCountProperty.getValue())
        .thenReturn(FAILURE_DETECTION_COUNT);
  }

  private void initializePlugin() {
    plugin.init(connection, propertySet, hostInfo, mockPlugin, logger, monitorServiceInitializer);
  }
}
