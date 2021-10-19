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
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.ha.ca.BasicConnectionProvider;
import com.mysql.cj.log.Log;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DefaultMonitorService implements IMonitorService {
  static final Map<String, IMonitor> MONITOR_MAP = new ConcurrentHashMap<>();
  static final Map<IMonitor, Future<?>> TASKS_MAP = new ConcurrentHashMap<>();
  static ExecutorService threadPool; // Effectively final.

  private final Log log;
  final IMonitorInitializer monitorInitializer;
  final IExecutorServiceInitializer executorServiceInitializer;

  public DefaultMonitorService(Log log) {
    this(
        (hostInfo, propertySet) -> new Monitor(
            new BasicConnectionProvider(),
            hostInfo,
            propertySet,
            log),
        Executors::newCachedThreadPool,
        log
    );
  }

  DefaultMonitorService(
      IMonitorInitializer monitorInitializer,
      IExecutorServiceInitializer executorServiceInitializer,
      Log log) {

    this.monitorInitializer = monitorInitializer;
    this.executorServiceInitializer = executorServiceInitializer;
    this.log = log;
  }

  @Override
  public MonitorConnectionContext startMonitoring(
      Set<String> nodeKeys,
      HostInfo hostInfo,
      PropertySet propertySet,
      int failureDetectionTimeMillis,
      int failureDetectionIntervalMillis,
      int failureDetectionCount) {

    if (nodeKeys.isEmpty()) {
      log.logWarn("Passed in empty NodeKey Set. Set should not be empty");
      throw new IllegalArgumentException("Empty NodeKey set passed into DefaultMonitorService");
    }

    final IMonitor monitor = getMonitor(nodeKeys, hostInfo, propertySet);

    if (threadPool == null) {
      threadPool = executorServiceInitializer.createExecutorService();
    }

    final MonitorConnectionContext context = new MonitorConnectionContext(
        nodeKeys,
        log,
        failureDetectionTimeMillis,
        failureDetectionIntervalMillis,
        failureDetectionCount);

    monitor.startMonitoring(context);
    TASKS_MAP.computeIfAbsent(monitor, k -> threadPool.submit(monitor));

    return context;
  }

  @Override
  public void stopMonitoring(MonitorConnectionContext context) {
    final Iterator<String> keys = context.getNodeKeys().iterator();
    final IMonitor monitor = MONITOR_MAP.get(keys.next());
    monitor.stopMonitoring(context);
  }

  protected IMonitor getMonitor(Set<String> nodeKeys, HostInfo hostInfo, PropertySet propertySet) {
    final String node = nodeKeys.stream().filter(MONITOR_MAP::containsKey).findFirst().orElse(nodeKeys.iterator().next());
    final IMonitor monitor = MONITOR_MAP.computeIfAbsent(node, k -> monitorInitializer.createMonitor(hostInfo, propertySet));

    for (String nodeKey : nodeKeys) {
      MONITOR_MAP.putIfAbsent(nodeKey, monitor);
    }

    return monitor;
  }
}
