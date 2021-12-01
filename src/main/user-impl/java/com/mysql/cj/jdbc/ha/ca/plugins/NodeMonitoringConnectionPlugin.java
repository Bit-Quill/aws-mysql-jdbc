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

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ha.ca.ClusterAwareConnectionProxy;
import com.mysql.cj.log.Log;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

public class NodeMonitoringConnectionPlugin implements IConnectionPlugin {

  static final int CHECK_INTERVAL_MILLIS = 1000;
  private static final String RETRIEVE_HOST_PORT_SQL = "SELECT CONCAT(@@hostname, ':', @@port)";

  protected IConnectionPlugin nextPlugin;
  protected Log log;
  protected PropertySet propertySet;
  private IMonitorService monitorService;
  private final IMonitorServiceInitializer monitorServiceInitializer;
  private final Set<String> nodeKeys = new HashSet<>();
  private final ICurrentConnectionProvider currentConnectionProvider;
  private JdbcConnection connection;

  @FunctionalInterface
  interface IMonitorServiceInitializer {
    IMonitorService create(Log log);
  }

  public NodeMonitoringConnectionPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log log) {
    this(
        currentConnectionProvider,
        propertySet,
        nextPlugin,
        log,
        DefaultMonitorService::new);
  }

  NodeMonitoringConnectionPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log log,
      IMonitorServiceInitializer monitorServiceInitializer) {
    assertArgumentIsNotNull(currentConnectionProvider, "currentConnectionProvider");
    assertArgumentIsNotNull(propertySet, "propertySet");
    assertArgumentIsNotNull(nextPlugin, "next");
    assertArgumentIsNotNull(log, "log");

    this.currentConnectionProvider = currentConnectionProvider;
    this.connection = currentConnectionProvider.getCurrentConnection();
    this.propertySet = propertySet;
    this.log = log;
    this.nextPlugin = nextPlugin;
    this.monitorServiceInitializer =  monitorServiceInitializer;

    generateNodeKeys(this.currentConnectionProvider.getCurrentConnection());
  }

  /**
   * Executes the given SQL function with {@link Monitor} if connection monitoring is enabled.
   * Otherwise, executes the SQL function directly.
   *
   * @param methodInvokeOn Class of an object that method to monitor to be invoked on.
   * @param methodName     Name of the method to monitor.
   * @param executeSqlFunc {@link Callable} SQL function.
   * @return Results of the {@link Callable} SQL function.
   * @throws Exception if an error occurs.
   */
  @Override
  public Object execute(Class<?> methodInvokeOn, String methodName, Callable<?> executeSqlFunc) throws Exception {
    // update config settings since they may change
    final boolean isEnabled = this.propertySet
        .getBooleanProperty(PropertyKey.nativeFailureDetectionEnabled)
        .getValue();

    if (!isEnabled || !this.doesNeedMonitoring(methodInvokeOn, methodName)) {
      // do direct call
      return this.nextPlugin.execute(methodInvokeOn, methodName, executeSqlFunc);
    }
    // ... otherwise, use a separate thread to execute method

    final int failureDetectionTimeMillis = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionTime)
        .getValue();
    final int failureDetectionIntervalMillis = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionInterval)
        .getValue();
    final int failureDetectionCount = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionCount)
        .getValue();

    initMonitorService();

    Object result;
    MonitorConnectionContext monitorContext = null;

    try {
      this.log.logTrace(String.format(
          "[NodeMonitoringConnectionPlugin.execute]: method=%s.%s, monitoring is activated",
              methodInvokeOn.getName(), methodName));

      this.checkIfChanged(this.currentConnectionProvider.getCurrentConnection());

      monitorContext = this.monitorService.startMonitoring(
          this.connection, //abort current connection if needed
          this.nodeKeys,
          this.currentConnectionProvider.getCurrentHostInfo(),
          this.propertySet,
          failureDetectionTimeMillis,
          failureDetectionIntervalMillis,
          failureDetectionCount);

      result = this.nextPlugin.execute(methodInvokeOn, methodName, executeSqlFunc);

    } catch (SQLException | CJException exception) {
      throw exception;
    } catch (Exception ex) {
      if (monitorContext != null && monitorContext.isNodeUnhealthy()) {
        // method execution has been interrupted by monitoring thread
        throw new CJCommunicationsException("Node is unavailable.", ex);
      } else {
        throw ex;
      }
    } finally {
      if (monitorContext != null) {
        this.monitorService.stopMonitoring(monitorContext);
        synchronized (monitorContext) {
          if (monitorContext.isNodeUnhealthy() && !this.connection.isClosed()) {
            abortConnection();
            throw new CJCommunicationsException("Node is unavailable.");
          }
        }
      }
      this.log.logTrace(String.format(
          "[NodeMonitoringConnectionPlugin.execute]: method=%s.%s, monitoring is deactivated",
              methodInvokeOn.getName(), methodName));
    }

    return result;
  }

  void abortConnection() {
    try {
      this.connection.abortInternal();
    } catch (SQLException sqlEx) {
      // ignore
    }
  }

  protected boolean doesNeedMonitoring(Class<?> methodInvokeOn, String methodName) {
    // It's possible to use the following, or similar, expressions to verify method invocation class
    //
    // boolean isJdbcConnection = JdbcConnection.class.isAssignableFrom(methodInvokeOn) || ClusterAwareConnectionProxy.class.isAssignableFrom(methodInvokeOn);
    // boolean isJdbcStatement = Statement.class.isAssignableFrom(methodInvokeOn);
    // boolean isJdbcResultSet = ResultSet.class.isAssignableFrom(methodInvokeOn);

    if ("close".equals(methodName) || methodName.startsWith("get") || methodName.startsWith("abort")) {
      return false;
    }

    // Monitor all other methods
    return true;
  }

  private void initMonitorService() {
    if (this.monitorService == null) {
      this.monitorService = this.monitorServiceInitializer.create(this.log);
    }
  }

  @Override
  public void releaseResources() {
    if (this.monitorService != null) {
      this.monitorService.releaseResources();
    }

    this.monitorService = null;
    this.nextPlugin.releaseResources();
  }

  private void assertArgumentIsNotNull(Object param, String paramName) {
    if (param == null) {
      throw new NullArgumentException(paramName);
    }
  }

  /**
   * Check if the connection has changed.
   * If so, remove monitor's references to that node and
   * regenerate the set of node keys referencing the node we need to monitor.
   *
   * @param newConnection The connection used by {@link ClusterAwareConnectionProxy}.
   */
  private void checkIfChanged(JdbcConnection newConnection) {
    final boolean isSameConnection = this.connection.equals(newConnection);
    if (!isSameConnection) {
      this.monitorService.stopMonitoringForAllConnections(this.nodeKeys);
      this.connection = newConnection;
      generateNodeKeys(this.connection);
    }
  }

  /**
   * Generate a set of node keys representing the node to monitor.
   *
   * @param connection the connection to a specific Aurora node.
   */
  private void generateNodeKeys(Connection connection) {
    this.nodeKeys.clear();
    final HostInfo hostInfo = this.currentConnectionProvider.getCurrentHostInfo();
    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(RETRIEVE_HOST_PORT_SQL)) {
        while (rs.next()) {
          this.nodeKeys.add(rs.getString(1));
        }
      }
    } catch (SQLException sqlException) {
      // log and ignore
      this.log.logTrace(
          "[NodeMonitoringConnectionPlugin.initNodes]: Could not retrieve Host:Port from querying");
    }

    this.nodeKeys.add(
        String.format("%s:%s",
            hostInfo.getHost(),
            hostInfo.getPort()
        ));
  }
}
