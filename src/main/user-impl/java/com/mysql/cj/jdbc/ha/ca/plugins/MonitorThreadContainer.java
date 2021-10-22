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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class MonitorThreadContainer {
    private static MonitorThreadContainer singleton = null;
    private static final AtomicInteger CLASS_USAGE_COUNT = new AtomicInteger();
    private final Map<String, IMonitor> monitorMap = new ConcurrentHashMap<>();
    private final Map<IMonitor, Future<?>> tasksMap = new ConcurrentHashMap<>();
    private final ExecutorService threadPool;

    public static MonitorThreadContainer getInstance() {
        return getInstance(null);
    }

    public static synchronized MonitorThreadContainer getInstance(IExecutorServiceInitializer executorServiceInitializer) {
        if (singleton == null) {
            singleton = new MonitorThreadContainer(executorServiceInitializer);
            CLASS_USAGE_COUNT.set(0);
        }
        CLASS_USAGE_COUNT.getAndIncrement();
        return singleton;
    }

    public static synchronized void releaseInstance() {
        if (singleton == null) {
            return;
        }

        if (CLASS_USAGE_COUNT.decrementAndGet() <= 0) {
            singleton.releaseResources();
            singleton = null;
        }
    }

    private MonitorThreadContainer(IExecutorServiceInitializer executorServiceInitializer) {
        this.threadPool = executorServiceInitializer.createExecutorService();
    }

    public Map<String, IMonitor> getMonitorMap() {
        return monitorMap;
    }

    public Map<IMonitor, Future<?>> getTasksMap() {
        return tasksMap;
    }

    public ExecutorService getThreadPool() {
        return threadPool;
    }

    private synchronized void releaseResources() {
        tasksMap.values().stream()
            .filter(val -> !val.isDone() && !val.isCancelled())
            .forEach(val -> val.cancel(true));

        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }
}
