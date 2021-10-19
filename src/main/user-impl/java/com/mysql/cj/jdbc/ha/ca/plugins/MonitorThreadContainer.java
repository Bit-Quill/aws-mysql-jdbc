package com.mysql.cj.jdbc.ha.ca.plugins;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class MonitorThreadContainer {
    private static MonitorThreadContainer singleton = null;
    private static final AtomicInteger classUsage = new AtomicInteger();
    private final Map<String, IMonitor> MONITOR_MAP = new ConcurrentHashMap<>();
    private final Map<IMonitor, Future<?>> TASKS_MAP = new ConcurrentHashMap<>();
    private final ExecutorService threadPool;

    public static MonitorThreadContainer getInstance() {
        return getInstance(null);
    }

    public static synchronized MonitorThreadContainer getInstance(IExecutorServiceInitializer executorServiceInitializer) {
        if (singleton == null) {
            singleton = new MonitorThreadContainer(executorServiceInitializer);
        }
        classUsage.getAndIncrement();
        return singleton;
    }

    public static synchronized void releaseInstance() {
        if (classUsage.decrementAndGet() <= 0) {
            singleton.releaseResources();
            singleton = null;
        }
    }

    private MonitorThreadContainer(IExecutorServiceInitializer executorServiceInitializer) {
        this.threadPool = executorServiceInitializer.createExecutorService();
    }

    public Map<String, IMonitor> getMonitorMap() {
        return MONITOR_MAP;
    }

    public Map<IMonitor, Future<?>> getTasksMap() {
        return TASKS_MAP;
    }

    public ExecutorService getThreadPool() {
        return threadPool;
    }

    private synchronized void releaseResources() {
        TASKS_MAP.values().stream()
            .filter(val -> !val.isDone() && !val.isCancelled())
            .forEach(val -> val.cancel(true));

        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }
}
