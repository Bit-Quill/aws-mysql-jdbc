package com.mysql.cj.jdbc.ha.plugins.failover;

import com.mysql.cj.log.Log;
import com.mysql.cj.util.StringUtils;

public class ClusterAwareMetricReporter {
    private static ClusterAwareMetricContainer metricContainer = ClusterAwareMetricContainer.getInstance();

    public static void reportMetrics(String connUrl, Log log, boolean atClusterLevel) {
        if (StringUtils.isNullOrEmpty(connUrl) || log == null) {
            return;
        }
        final ClusterAwareMetrics metrics = metricContainer.getOrCreate(connUrl, atClusterLevel);
        StringBuilder logMessage = new StringBuilder(256);

        logMessage.append("** Performance Metrics Report for '");
        logMessage.append(connUrl);
        logMessage.append("' **\n");
        log.logInfo(logMessage);
        metrics.reportMetrics(log);
    }

    public static void resetMetrics(String connUrl, boolean atClusterLevel) {
        if (StringUtils.isNullOrEmpty(connUrl)) {
            return;
        }
        final ClusterAwareMetrics metrics = metricContainer.getOrCreate(connUrl, atClusterLevel);
        metrics.resetMetrics();
    }

    public static void resetMetrics(String connUrl) {
        resetMetrics(connUrl, true);
    }
}
