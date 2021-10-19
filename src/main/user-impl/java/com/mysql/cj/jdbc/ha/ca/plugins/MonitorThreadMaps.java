package com.mysql.cj.jdbc.ha.ca.plugins;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class MonitorThreadMaps {
    private static MonitorThreadMaps singleton = null;
    static final Map<String, IMonitor> MONITOR_MAP = new ConcurrentHashMap<>();
    static final Map<IMonitor, Future<?>> TASKS_MAP = new ConcurrentHashMap<>();
    static ExecutorService threadPool; // Effectively final.
    static final AtomicInteger classUseCounter = new AtomicInteger();
    static IExecutorServiceInitializer execServInit;

    public static MonitorThreadMaps getInstance() {
        return getInstance(null);
    }

    public static MonitorThreadMaps getInstance(IExecutorServiceInitializer executorServiceInitializer) {
        if (singleton == null) {
            singleton = new MonitorThreadMaps();
            if (executorServiceInitializer != null) {
                execServInit = executorServiceInitializer;
            }
        }
        return singleton;
    }

    Map<String, IMonitor> getMonitorMap() {
        return MONITOR_MAP;
    }

    Map<IMonitor, Future<?>> getTasksMap() {
        return TASKS_MAP;
    }

     ExecutorService getThreadPool() {
        if (this.threadPool == null) {
            this.threadPool = execServInit.createExecutorService();
        }
        classUseCounter.getAndIncrement();
        return threadPool;
    }

    void releaseMaps() {
        if (classUseCounter.decrementAndGet() <= 0) {
            TASKS_MAP.values().stream()
                .filter(val -> !(val.isDone() || val.isCancelled()))
                .forEach(val -> val.cancel(true));

            if (threadPool != null) {
                threadPool.shutdownNow();
                threadPool = null;
            }
        }
    }
}
