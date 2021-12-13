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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.when;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ha.ca.ClusterAwareConnectionProxy;
import com.mysql.cj.log.Log;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * Initialize constants and mock variables common to tests for {@link NodeMonitoringConnectionPlugin}.
 */
public class NodeMonitoringConnectionPluginBaseTest {
  @Mock ClusterAwareConnectionProxy proxy;
  @Mock JdbcConnection connection;
  @Mock Statement statement;
  @Mock ResultSet resultSet;
  @Mock PropertySet propertySet;
  @Mock HostInfo hostInfo;
  @Mock IConnectionPlugin mockPlugin;
  @Mock Log logger;
  @Mock Supplier<IMonitorService> supplier;
  @Mock MonitorConnectionContext context;
  @Mock IMonitorService monitorService;
  @Mock Callable sqlFunction;
  @Mock RuntimeProperty<Boolean> failureDetectionEnabledProperty;
  @Mock RuntimeProperty<Integer> failureDetectionTimeProperty;
  @Mock RuntimeProperty<Integer> failureDetectionIntervalProperty;
  @Mock RuntimeProperty<Integer> failureDetectionCountProperty;

  static final String NODE = "node";
  static final Class MONITOR_METHOD_INVOKE_ON = JdbcConnection.class;
  static final String MONITOR_METHOD_NAME = "executeQuery";
  static final String NO_MONITOR_METHOD_NAME = "foo";
  static final int FAILURE_DETECTION_TIME = 10;
  static final int FAILURE_DETECTION_INTERVAL = 100;
  static final int FAILURE_DETECTION_COUNT = 5;

  void initDefaultMockReturns() throws Exception {
    when(hostInfo.getHost())
        .thenReturn(NODE);
    when(supplier.get())
        .thenReturn(monitorService);
    when(monitorService.startMonitoring(
        any(JdbcConnection.class),
        anySet(),
        any(HostInfo.class),
        any(PropertySet.class),
        anyInt(),
        anyInt(),
        anyInt()))
        .thenReturn(context);

    when(mockPlugin.execute(any(Class.class), anyString(), Mockito.any(Callable.class))).thenReturn("done");

    when(proxy.getCurrentConnection()).thenReturn(connection);
    when(proxy.getCurrentHostInfo()).thenReturn(hostInfo);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery(anyString())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);
    when(hostInfo.getHost()).thenReturn("host");
    when(hostInfo.getHost()).thenReturn("port");

    when(propertySet.getBooleanProperty(Mockito.eq(PropertyKey.failureDetectionEnabled)))
        .thenReturn(failureDetectionEnabledProperty);
    when(propertySet.getIntegerProperty(Mockito.eq(PropertyKey.failureDetectionTime)))
        .thenReturn(failureDetectionTimeProperty);
    when(propertySet.getIntegerProperty(Mockito.eq(PropertyKey.failureDetectionInterval)))
        .thenReturn(failureDetectionIntervalProperty);
    when(propertySet.getIntegerProperty(Mockito.eq(PropertyKey.failureDetectionCount)))
        .thenReturn(failureDetectionCountProperty);

    when(failureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);
    when(failureDetectionTimeProperty.getValue())
        .thenReturn(FAILURE_DETECTION_TIME);
    when(failureDetectionIntervalProperty.getValue())
        .thenReturn(FAILURE_DETECTION_INTERVAL);
    when(failureDetectionCountProperty.getValue())
        .thenReturn(FAILURE_DETECTION_COUNT);
  }
}
