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
      Set<String> nodeKey,
      HostInfo hostInfo,
      PropertySet propertySet,
      int failureDetectionTimeMillis,
      int failureDetectionIntervalMillis,
      int failureDetectionCount) {

    IMonitor monitor = null;
    Iterator<String> iter = nodeKey.iterator();
    while (iter.hasNext() && monitor == null) {
      monitor = MONITOR_MAP.get(iter.next());
    }
    iter = nodeKey.iterator();
    if (monitor == null) {
      monitor = MONITOR_MAP.computeIfAbsent(iter.next(),
          k -> monitorInitializer.createMonitor(hostInfo, propertySet));
    }
    final IMonitor finalMonitor = monitor;
    while (iter.hasNext()) {
      MONITOR_MAP.computeIfAbsent(iter.next(),
          k -> finalMonitor);
    }

    if (threadPool == null) {
      threadPool = executorServiceInitializer.createExecutorService();
    }

    final MonitorConnectionContext context = new MonitorConnectionContext(
        nodeKey,
        log,
        failureDetectionTimeMillis,
        failureDetectionIntervalMillis,
        failureDetectionCount);

    finalMonitor.startMonitoring(context);
    TASKS_MAP.computeIfAbsent(finalMonitor, k -> threadPool.submit(finalMonitor));

    return context;
  }

  @Override
  public void stopMonitoring(MonitorConnectionContext context) {
    Iterator<String> keys = context.getNode().iterator();
    final IMonitor monitor = MONITOR_MAP.get(keys.next());
    monitor.stopMonitoring(context);
  }
}