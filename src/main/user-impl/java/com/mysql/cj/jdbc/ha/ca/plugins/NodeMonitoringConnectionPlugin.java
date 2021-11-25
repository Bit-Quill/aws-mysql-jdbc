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
import com.mysql.cj.jdbc.ha.ca.ClusterAwareConnectionProxy;
import com.mysql.cj.log.Log;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class NodeMonitoringConnectionPlugin implements IConnectionPlugin {

  static final int CHECK_INTERVAL_MILLIS = 1000;
  private static final String RETRIEVE_HOST_PORT_SQL = "SELECT CONCAT(@@hostname, ':', @@port)";

  protected IConnectionPlugin nextPlugin;
  protected Log log;
  protected PropertySet propertySet;
  private IMonitorService monitorService;
  private final IMonitorServiceInitializer monitorServiceInitializer;
  private MonitorConnectionContext monitorContext;
  private final Set<String> nodeKeys = new HashSet<>();
  private final ClusterAwareConnectionProxy proxy;
  private Connection connection;

  @FunctionalInterface
  interface IMonitorServiceInitializer {
    IMonitorService create(Log log);
  }

  public NodeMonitoringConnectionPlugin(
      ClusterAwareConnectionProxy proxy,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log log) {
    this(
        proxy,
        propertySet,
        nextPlugin,
        log,
        DefaultMonitorService::new);
  }

  NodeMonitoringConnectionPlugin(
      ClusterAwareConnectionProxy proxy,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log log,
      IMonitorServiceInitializer monitorServiceInitializer) {
    assertArgumentIsNotNull(proxy, "proxy");
    assertArgumentIsNotNull(propertySet, "propertySet");
    assertArgumentIsNotNull(nextPlugin, "next");
    assertArgumentIsNotNull(log, "log");

    this.proxy = proxy;
    this.connection = proxy.getCurrentConnection();
    this.propertySet = propertySet;
    this.log = log;
    this.nextPlugin = nextPlugin;
    this.monitorServiceInitializer =  monitorServiceInitializer;

    generateNodeKeys(this.proxy.getCurrentConnection());
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
    ExecutorService executor = null;
    try {
      this.log.logTrace(String.format(
          "[NodeMonitoringConnectionPlugin.execute]: method=%s.%s, monitoring is activated",
              methodInvokeOn.getName(), methodName));

      checkNewConnection(this.proxy.getCurrentConnection());

      this.monitorContext = this.monitorService.startMonitoring(
          this.nodeKeys,
          this.proxy.getCurrentHostInfo(),
          this.propertySet,
          failureDetectionTimeMillis,
          failureDetectionIntervalMillis,
          failureDetectionCount);

      executor = Executors.newSingleThreadExecutor();
      final Future<Object> executeFuncFuture = executor.submit(() -> this.nextPlugin.execute(methodInvokeOn, methodName, executeSqlFunc));
      executor.shutdown(); // stop executor to accept new tasks

      boolean isDone = executeFuncFuture.isDone();
      while (!isDone) {
        TimeUnit.MILLISECONDS.sleep(CHECK_INTERVAL_MILLIS);
        isDone = executeFuncFuture.isDone();

        if (this.monitorContext.isNodeUnhealthy()) {
          //throw new SocketTimeoutException("Read time out");
          throw new CJCommunicationsException("Node is unavailable.");
        }
      }

      result = executeFuncFuture.get();
    } catch (ExecutionException exception) {
      final Throwable throwable = exception.getCause();
      if (throwable instanceof Error) {
        throw (Error) throwable;
      }
      throw (Exception) throwable;
    } finally {
      this.monitorService.stopMonitoring(this.monitorContext);
      if (executor != null) {
        executor.shutdownNow();
      }
      this.log.logTrace(String.format(
          "[NodeMonitoringConnectionPlugin.execute]: method=%s.%s, monitoring is deactivated",
              methodInvokeOn.getName(), methodName));
    }

    return result;
  }

  protected boolean doesNeedMonitoring(Class<?> methodInvokeOn, String methodName) {
    // It's possible to use the following, or similar, expressions to verify method invocation class
    //
    // boolean isJdbcConnection = JdbcConnection.class.isAssignableFrom(methodInvokeOn) || ClusterAwareConnectionProxy.class.isAssignableFrom(methodInvokeOn);
    // boolean isJdbcStatement = Statement.class.isAssignableFrom(methodInvokeOn);
    // boolean isJdbcResultSet = ResultSet.class.isAssignableFrom(methodInvokeOn);

    final List<String> methodsStartingWith = Arrays.asList("get", "abort");
    for (final String method : methodsStartingWith) {
      if (methodName.startsWith(method)) {
        return false;
      }
    }

    final List<String> methodsEqualTo = Arrays.asList("close", "next");
    for (final String method : methodsEqualTo) {
      if (method.equals(methodName)) {
        return false;
      }
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
  private void checkNewConnection(Connection newConnection) {
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
    final HostInfo hostInfo = this.proxy.getCurrentHostInfo();
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
