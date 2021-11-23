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
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.jdbc.ha.ca.ClusterAwareConnectionProxy;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.NullLogger;
import org.jboss.invocation.InvocationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NodeMonitoringConnectionPluginTest extends NodeMonitoringConnectionPluginBaseTest {
  private static final ExecutionException EXECUTION_EXCEPTION = new ExecutionException(
      "exception",
      new InvocationException("exception", new Throwable()));

  private NodeMonitoringConnectionPlugin plugin;
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
      final ClusterAwareConnectionProxy proxy,
      final PropertySet set,
      final IConnectionPlugin connectionPlugin,
      final Log log) {
    assertThrows(
        NullArgumentException.class,
        () -> new NodeMonitoringConnectionPlugin(proxy, set, connectionPlugin, log));
  }

  @Test
  void test_2_executeWithFailoverDisabled() throws Exception {
    when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.FALSE);

    initializePlugin();
    plugin.execute(MONITOR_METHOD_INVOKE_ON, MONITOR_METHOD_NAME, sqlFunction);

    verify(initializer, never()).create(any());
    verify(mockPlugin).execute(any(Class.class), eq(MONITOR_METHOD_NAME), eq(sqlFunction));
  }

  @Test
  void test_3_executeWithNoNeedToMonitor() throws Exception {
    when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);

    initializePlugin();
    plugin.execute(MONITOR_METHOD_INVOKE_ON, NO_MONITOR_METHOD_NAME, sqlFunction);

    verify(initializer, atMostOnce()).create(logger);
    verify(mockPlugin).execute(any(Class.class), eq(NO_MONITOR_METHOD_NAME), eq(sqlFunction));
  }

  @Test
  void test_4_executeThrowsExecutionException() {
    when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);

    initializePlugin();

    assertThrows(Exception.class, () -> {
      when(mockPlugin.execute(any(Class.class), any(), any()))
          .thenThrow(EXECUTION_EXCEPTION);

      plugin.execute(MONITOR_METHOD_INVOKE_ON, MONITOR_METHOD_NAME, sqlFunction);
    });

    verify(monitorService).stopMonitoring(eq(context));
  }

  @Test
  void test_5_executeWithUnhealthyNode() {
    when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);

    initializePlugin();

    assertThrows(CJCommunicationsException.class, () -> {
      when(context.isNodeUnhealthy())
          .thenReturn(Boolean.TRUE);

      when(mockPlugin.execute(any(Class.class), any(), any()))
          .thenAnswer(invocation -> {
            // Imitate running a long query;
            Thread.sleep(60 * 1000);
            return "done";
          });

      plugin.execute(MONITOR_METHOD_INVOKE_ON, MONITOR_METHOD_NAME, sqlFunction);
    });

    verify(context).isNodeUnhealthy();
    verify(monitorService).stopMonitoring(eq(context));
  }

  @Test
  void test_6_executeSuccessfulWithLongQuery() throws Exception {
    final String expected = "foo";

    when(nativeFailureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);
    when(context.isNodeUnhealthy())
        .thenReturn(Boolean.FALSE);
    when(mockPlugin.execute(any(Class.class), any(), any()))
        .thenAnswer(invocation -> {
          // Imitate running a query for 10 seconds.
          Thread.sleep(10 * 1000);
          return expected;
        });

    initializePlugin();
    final Object result = plugin.execute(MONITOR_METHOD_INVOKE_ON, MONITOR_METHOD_NAME, sqlFunction);

    assertEquals(expected, result);
    verify(context, atLeastOnce()).isNodeUnhealthy();
    verify(monitorService).stopMonitoring(eq(context));
  }

  /**
   * Generate different sets of method arguments where one argument is null to ensure
   * {@link NodeMonitoringConnectionPlugin#NodeMonitoringConnectionPlugin(ClusterAwareConnectionProxy, PropertySet, IConnectionPlugin, Log)}
   * can handle null arguments correctly.
   *
   * @return different sets of arguments.
   */
  private static Stream<Arguments> generateNullArguments() {
    final ClusterAwareConnectionProxy proxy = mock(ClusterAwareConnectionProxy.class);
    final PropertySet set = new DefaultPropertySet();
    final Log log = new NullLogger("NodeMonitoringConnectionPluginTest");
    final IConnectionPlugin connectionPlugin = new DefaultConnectionPlugin(log);

    return Stream.of(
        Arguments.of(null, set, connectionPlugin, log),
        Arguments.of(proxy, null, connectionPlugin, log),
        Arguments.of(proxy, set, null, log),
        Arguments.of(proxy, set, connectionPlugin, null)
    );
  }

  private void initializePlugin() {
    plugin = new NodeMonitoringConnectionPlugin(proxy, propertySet, mockPlugin, logger, initializer);
  }
}
