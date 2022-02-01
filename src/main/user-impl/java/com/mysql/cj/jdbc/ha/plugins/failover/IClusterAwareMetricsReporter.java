package com.mysql.cj.jdbc.ha.plugins.failover;

import com.mysql.cj.log.Log;
import com.mysql.cj.util.StringUtils;

public interface IClusterAwareMetricsReporter {
    static void reportMetrics(String connUrl, Log log) {
        reportMetrics(connUrl, log, false);
    }

    static void reportMetrics(String connUrl, Log log, boolean atInstance) {
        if (StringUtils.isNullOrEmpty(connUrl) || log == null) {
            return;
        }

        new ClusterAwareMetricsContainer().reportMetrics(connUrl, log);
    }

    static void resetMetrics() {
        new ClusterAwareMetricsContainer().resetMetrics();
    }
}
