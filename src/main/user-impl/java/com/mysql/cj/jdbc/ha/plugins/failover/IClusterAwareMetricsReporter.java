package com.mysql.cj.jdbc.ha.plugins.failover;

import com.mysql.cj.log.Log;
import com.mysql.cj.util.StringUtils;

public interface IClusterAwareMetricsReporter {

    /**
     * Reports collected failover performance metrics at cluster level to a provided logger.
     *
     * @param connUrl the connection URL to report.
     * @param log A logger to report collected metric.
     */
    static void reportMetrics(String connUrl, Log log) {
        reportMetrics(connUrl, log, false);
    }

    /**
     * Reports collected failover performance metrics to a provided logger.
     *
     * @param connUrl the connection URL to report.
     * @param log logger to report collected metric.
     * @param atInstance whether to print instance or cluster performance metrics.
     */
    static void reportMetrics(String connUrl, Log log, boolean atInstance) {
        if (StringUtils.isNullOrEmpty(connUrl) || log == null) {
            return;
        }

        ClusterAwareMetricsContainer.reportMetrics(connUrl, log, atInstance);
    }

    /**
     * Resets all collected failover performance metrics
     */
    static void resetMetrics() {
        ClusterAwareMetricsContainer.resetMetrics();
    }
}
