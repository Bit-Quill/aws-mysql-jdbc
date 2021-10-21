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

import com.mysql.cj.Messages;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.ha.ca.BasicConnectionProvider;
import com.mysql.cj.log.Log;
import org.jboss.util.NullArgumentException;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DefaultMonitorService implements IMonitorService {
  MonitorThreadContainer threadContainer;

  private final Log log;
  final IMonitorInitializer monitorInitializer;

  public DefaultMonitorService(Log log) {
    this(
        (hostInfo, propertySet) -> new Monitor(
            new BasicConnectionProvider(),
            hostInfo,
            propertySet,
            propertySet.getIntegerProperty(PropertyKey.monitorDisposeTime).getValue(),
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
    this.log = log;
    this.threadContainer = MonitorThreadContainer.getInstance(executorServiceInitializer);
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
      final String warning = Messages.getString("DefaultMonitorService.EmptyNodeKeys");
      log.logWarn(warning);
      throw new IllegalArgumentException(warning);
    }

    final IMonitor monitor = getMonitor(nodeKeys, hostInfo, propertySet);

    final MonitorConnectionContext context = new MonitorConnectionContext(
        nodeKeys,
        log,
        failureDetectionTimeMillis,
        failureDetectionIntervalMillis,
        failureDetectionCount);

    monitor.startMonitoring(context);
    this.threadContainer.getTasksMap().computeIfAbsent(monitor, k -> this.threadContainer.getThreadPool().submit(monitor));

    return context;
  }

  @Override
  public void stopMonitoring(MonitorConnectionContext context) {
    final String node = context.getNodeKeys()
        .stream().filter(this.threadContainer.getMonitorMap()::containsKey)
        .findAny()
        .orElseThrow(() -> new NoSuchElementException("Could not find any NodeKey from context."));

    final IMonitor monitor = this.threadContainer.getMonitorMap().get(node);
    monitor.stopMonitoring(context);
  }

  @Override
  public void releaseResources() {
    this.threadContainer = null;
    MonitorThreadContainer.releaseInstance();
  }

  @Override
  public synchronized void removeMonitorFromMap(IMonitor monitor) {
    if (monitor == null) {
      throw new NullArgumentException("Cannot remove a null monitor");
    }

    final Map<String, IMonitor> monitorMap = MonitorThreadContainer.getInstance().getMonitorMap();
    monitorMap.forEach((key, value) -> {
      if (value.equals(monitor)) {
        monitorMap.remove(key);
      }
    });

    final Map<IMonitor, Future<?>> taskMap = MonitorThreadContainer.getInstance().getTasksMap();
    taskMap.forEach((key, value) -> {
      if (key.equals(monitor)) {
        value.cancel(true);
        taskMap.remove(key);
      }
    });
  }

  protected IMonitor getMonitor(Set<String> nodeKeys, HostInfo hostInfo, PropertySet propertySet) {
    final String node = nodeKeys.stream().filter(this.threadContainer.getMonitorMap()::containsKey).findFirst().orElse(nodeKeys.iterator().next());
    final IMonitor monitor = this.threadContainer.getMonitorMap().computeIfAbsent(node, k -> monitorInitializer.createMonitor(hostInfo, propertySet));
    monitor.setService(this);

    for (String nodeKey : nodeKeys) {
      this.threadContainer.getMonitorMap().putIfAbsent(nodeKey, monitor);
    }

    return monitor;
  }
}
