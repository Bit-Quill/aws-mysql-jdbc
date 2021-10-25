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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.NullLogger;
import org.jboss.invocation.InvocationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

class NodeMonitoringFailoverPluginTest extends NodeMonitoringFailoverPluginBaseTest {
  private static final ExecutionException EXECUTION_EXCEPTION = new ExecutionException(
      "exception",
      new InvocationException("exception", new Throwable()));

  private NodeMonitoringFailoverPlugin plugin;

  private AutoCloseable closeable;

  @BeforeEach
  void init() throws Exception {
    closeable = MockitoAnnotations.openMocks(this);

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
        () -> new NodeMonitoringFailoverPlugin(connection, set, info, failoverPlugin, log));
  }

  @Test
  void test_2_initWithFailoverEnabled() {
    when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);

    initializePlugin();

    verify(initializer).create(Mockito.eq(logger));
  }

  @Test
  void test_3_initWithFailoverDisabled() {
    when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.FALSE);

    initializePlugin();
    verify(initializer, Mockito.never()).create(Mockito.any());
  }

  @Test
  void test_4_executeWithFailoverDisabled() throws Exception {
    when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.FALSE);
    initializePlugin();

    plugin.execute(MONITOR_METHOD_NAME, sqlFunction);
    verify(mockPlugin).execute(Mockito.eq(MONITOR_METHOD_NAME), Mockito.eq(sqlFunction));
  }

  @Test
  void test_5_executeWithNoNeedToMonitor() throws Exception {
    when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);
    initializePlugin();

    plugin.execute(NO_MONITOR_METHOD_NAME, sqlFunction);
    verify(mockPlugin)
        .execute(Mockito.eq(NO_MONITOR_METHOD_NAME), Mockito.eq(sqlFunction));
  }

  @Test
  void test_6_executeThrowsExecutionException() {
    when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);

    initializePlugin();

    Assertions.assertThrows(Exception.class, () -> {
      when(mockPlugin.execute(Mockito.any(), Mockito.any()))
          .thenThrow(EXECUTION_EXCEPTION);

      plugin.execute(MONITOR_METHOD_NAME, sqlFunction);
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

      plugin.execute(MONITOR_METHOD_NAME, sqlFunction);
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

    final Object result = plugin.execute(MONITOR_METHOD_NAME, sqlFunction);
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
    final Log log = new NullLogger("NodeMonitoringFailoverPluginTest");
    final IFailoverPlugin failoverPlugin = new DefaultFailoverPlugin(log);

    return Stream.of(
        Arguments.of(null, set, info, failoverPlugin, log),
        Arguments.of(connection, null, info, failoverPlugin, log),
        Arguments.of(connection, set, null, failoverPlugin, log),
        Arguments.of(connection, set, info, null, log),
        Arguments.of(connection, set, info, failoverPlugin, null)
    );
  }

  private void initializePlugin() {
    plugin = new NodeMonitoringFailoverPlugin(connection, propertySet, hostInfo, mockPlugin, logger, initializer);
  }
}
