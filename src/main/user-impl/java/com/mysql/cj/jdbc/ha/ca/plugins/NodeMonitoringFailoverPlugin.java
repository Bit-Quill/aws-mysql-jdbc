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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class NodeMonitoringFailoverPlugin implements IFailoverPlugin {

  static final int CHECK_INTERVAL_MILLIS = 1000;
  static final String METHODS_TO_MONITOR = "executeQuery,";
  private static final String RETRIEVE_HOST_PORT_SQL = "SELECT CONCAT(@@hostname, ':', @@port)";

  protected IFailoverPlugin next;
  protected Log log;
  protected PropertySet propertySet;
  protected boolean isEnabled;
  protected int failureDetectionTimeMillis;
  protected int failureDetectionIntervalMillis;
  protected int failureDetectionCount;
  private IMonitorService monitorService;
  private MonitorConnectionContext monitorContext;
  private final Set<String> nodeKeys = new HashSet<>();
  private final ClusterAwareConnectionProxy proxy;
  private Connection connection;

  @FunctionalInterface
  interface IMonitorServiceInitializer {
    IMonitorService create(Log log);
  }

  public NodeMonitoringFailoverPlugin(
      ClusterAwareConnectionProxy proxy,
      PropertySet propertySet,
      IFailoverPlugin next,
      Log log) {
    this(
        proxy,
        propertySet,
        next,
        log,
        DefaultMonitorService::new);
  }

  NodeMonitoringFailoverPlugin(
      ClusterAwareConnectionProxy proxy,
      PropertySet propertySet,
      IFailoverPlugin next,
      Log log,
      IMonitorServiceInitializer monitorServiceInitializer) {
    assertArgumentIsNotNull(proxy, "proxy");
    assertArgumentIsNotNull(propertySet, "propertySet");
    assertArgumentIsNotNull(next, "next");
    assertArgumentIsNotNull(log, "log");

    this.proxy = proxy;
    this.connection = proxy.getCurrentConnection();
    this.propertySet = propertySet;
    this.log = log;
    this.next = next;

    this.isEnabled = this.propertySet
        .getBooleanProperty(PropertyKey.nativeFailureDetectionEnabled)
        .getValue();
    this.failureDetectionTimeMillis = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionTime)
        .getValue();
    this.failureDetectionIntervalMillis = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionInterval)
        .getValue();
    this.failureDetectionCount = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionCount)
        .getValue();

    if (this.isEnabled) {
      this.monitorService = monitorServiceInitializer.create(this.log);
    }
  }

  @Override
  public Object execute(String methodName, Callable executeSqlFunc) throws Exception {
    final boolean needMonitoring = METHODS_TO_MONITOR.contains(methodName + ",");

    if (!this.isEnabled || !needMonitoring) {
      // do direct call
      return this.next.execute(methodName, executeSqlFunc);
    }

    // update config settings since they may change
    this.isEnabled = this.propertySet
        .getBooleanProperty(PropertyKey.nativeFailureDetectionEnabled)
        .getValue();
    this.failureDetectionTimeMillis = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionTime)
        .getValue();
    this.failureDetectionIntervalMillis = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionInterval)
        .getValue();
    this.failureDetectionCount = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionCount)
        .getValue();

    // use a separate thread to execute method

    Object result;
    ExecutorService executor = null;
    try {
      this.log.logTrace(String.format(
          "[NodeMonitoringFailoverPlugin.execute]: method=%s, monitoring is activated",
          methodName));

      final Set<String> nodeKeys = getUpdatedNodeKeys(
          checkFailover(proxy.getCurrentConnection()),
          proxy.getCurrentHostInfo());

      this.monitorContext = this.monitorService.startMonitoring(
          nodeKeys,
          this.proxy.getCurrentHostInfo(),
          this.propertySet,
          this.failureDetectionTimeMillis,
          this.failureDetectionIntervalMillis,
          this.failureDetectionCount);

      executor = Executors.newSingleThreadExecutor();
      final Future<Object> executeFuncFuture = executor.submit(() -> this.next.execute(methodName, executeSqlFunc));
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
          "[NodeMonitoringFailoverPlugin.execute]: method=%s, monitoring is deactivated",
          methodName));
    }

    return result;
  }

  @Override
  public void releaseResources() {
    // releaseResources may be called multiple times throughout the failover process.
    if (this.monitorService != null) {
      this.monitorService.releaseResources();
    }

    this.monitorService = null;
    this.next.releaseResources();
  }

  /**
   * If the connection is using a different hostname, add that host name to the set.
   *
   * @param connection The current connection.
   * @param hostInfo   Information about the current connection.
   * @return Updated set of node keys.
   */
  protected Set<String> getUpdatedNodeKeys(Connection connection, HostInfo hostInfo) {
    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(RETRIEVE_HOST_PORT_SQL)) {
        while (rs.next()) {
          this.nodeKeys.add(rs.getString(1));
        }
      }
    } catch (SQLException sqlException) {
      // log and ignore
      this.log.logTrace(
          "[NodeMonitoringFailoverPlugin.initNodes]: Could not retrieve Host:Port from querying");
    }

    this.nodeKeys.add(
        String.format("%s:%s",
            hostInfo.getHost(),
            hostInfo.getPort()
        ));

    return this.nodeKeys;
  }

  private void assertArgumentIsNotNull(Object param, String paramName) {
    if (param == null) {
      throw new NullArgumentException(paramName);
    }
  }

  /**
   * Check if the connection has changed due to failover.
   * If so, clear the set of keys for the dead node
   * and remove monitor's references to that node.
   *
   * @param newConnection The connection used by {@link ClusterAwareConnectionProxy}.
   * @return the most up-to-date connection.
   */
  private Connection checkFailover(Connection newConnection) {
    final boolean isSameConnection = this.connection.equals(newConnection);
    if (!isSameConnection) {
      this.monitorService.stopMonitoringForAllConnections(this.nodeKeys);
      this.nodeKeys.clear();
      this.connection = newConnection;
    }

    return this.connection;
  }
}
