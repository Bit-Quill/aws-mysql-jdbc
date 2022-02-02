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

package com.mysql.cj.jdbc.ha.plugins.failover;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ha.plugins.ICurrentConnectionProvider;
import com.mysql.cj.log.Log;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterAwareMetricsContainer implements IClusterAwareMetricsContainer {
    // ClusterID, Metrics
    private final static Map<String, ClusterAwareMetrics> clusterMetrics = new ConcurrentHashMap<>();
    // Instance URL, Metrics
    private final static Map<String, ClusterAwareMetrics> instanceMetrics = new ConcurrentHashMap<>();

    private ICurrentConnectionProvider currentConnectionProvider;
    private PropertySet propertySet;
    private String clusterId = "";

    public ClusterAwareMetricsContainer(ICurrentConnectionProvider currentConnectionProvider, PropertySet propertySet) {
        this.currentConnectionProvider = currentConnectionProvider;
        this.propertySet = propertySet;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public void registerFailureDetectionTime(long timeMs) {
        if (!canGatherPerfMetrics()) {
            return;
        }

        getClusterMetrics(clusterId).registerFailureDetectionTime(timeMs);

        if (shouldGatherAddition()) {
            getInstanceMetrics(getCurrentConnUrl()).registerFailureDetectionTime(timeMs);
        }
    }

    public void registerWriterFailoverProcedureTime(long timeMs) {
        if (!canGatherPerfMetrics()) {
            return;
        }

        getClusterMetrics(clusterId).registerWriterFailoverProcedureTime(timeMs);

        if (shouldGatherAddition()) {
            getInstanceMetrics(getCurrentConnUrl()).registerWriterFailoverProcedureTime(timeMs);
        }
    }

    public void registerReaderFailoverProcedureTime(long timeMs) {
        if (!canGatherPerfMetrics()) {
            return;
        }

        getClusterMetrics(clusterId).registerReaderFailoverProcedureTime(timeMs);

        if (shouldGatherAddition()) {
            getInstanceMetrics(getCurrentConnUrl()).registerReaderFailoverProcedureTime(timeMs);
        }
    }

    public void registerFailoverConnects(boolean isHit) {
        if (!canGatherPerfMetrics()) {
            return;
        }

        getClusterMetrics(clusterId).registerFailoverConnects(isHit);

        if (shouldGatherAddition()) {
            getInstanceMetrics(getCurrentConnUrl()).registerFailoverConnects(isHit);
        }
    }

    public void registerInvalidInitialConnection(boolean isHit) {
        if (!canGatherPerfMetrics()) {
            return;
        }

        getClusterMetrics(clusterId).registerInvalidInitialConnection(isHit);

        if (shouldGatherAddition()) {
            getInstanceMetrics(getCurrentConnUrl()).registerInvalidInitialConnection(isHit);
        }
    }

    public void registerUseLastConnectedReader(boolean isHit) {
        if (!canGatherPerfMetrics()) {
            return;
        }

        getClusterMetrics(clusterId).registerUseLastConnectedReader(isHit);

        if (shouldGatherAddition()) {
            getInstanceMetrics(getCurrentConnUrl()).registerUseLastConnectedReader(isHit);
        }
    }

    public void registerUseCachedTopology(boolean isHit) {
        if (!canGatherPerfMetrics()) {
            return;
        }

        getClusterMetrics(clusterId).registerUseCachedTopology(isHit);

        if (shouldGatherAddition()) {
            getInstanceMetrics(getCurrentConnUrl()).registerUseCachedTopology(isHit);
        }
    }

    private boolean canGatherPerfMetrics() {
        if (propertySet != null) {
            return propertySet.getBooleanProperty(PropertyKey.gatherPerfMetrics.getKeyName()).getValue();
        }
        return false;
    }

    private boolean shouldGatherAddition() {
        if (propertySet != null) {
            return propertySet.getBooleanProperty(PropertyKey.gatherAdditionalMetricsOnInstance.getKeyName()).getValue();
        }
        return false;
    }

    private ClusterAwareMetrics getClusterMetrics(String key) {
        return clusterMetrics.computeIfAbsent(
            key,
            k -> new ClusterAwareMetrics());
    }

    private ClusterAwareMetrics getInstanceMetrics(String key) {
        return instanceMetrics.computeIfAbsent(
            key,
            k -> new ClusterAwareMetrics());
    }
    
    private String getCurrentConnUrl() {
        String currUrl = "";
        final JdbcConnection currConn = currentConnectionProvider.getCurrentConnection();
        if (currConn != null) {
            currUrl = currConn.getHostPortPair();
        }
        return currUrl;
    }

    public static void reportMetrics(String connUrl, Log log) {
        reportMetrics(connUrl, log, false);
    }

    public static void reportMetrics(String connUrl, Log log, boolean forInstances) {
        final ClusterAwareMetrics metrics = forInstances ? instanceMetrics.get(connUrl) : clusterMetrics.get(connUrl);

        if (metrics != null) {
            StringBuilder logMessage = new StringBuilder(256);

            logMessage.append("** Performance Metrics Report for '")
                .append(connUrl)
                .append("' **\n");
            log.logInfo(logMessage);

            metrics.reportMetrics(log);
        } else {
            StringBuilder logMessage = new StringBuilder();
            logMessage.append("** No metrics collected for '")
                .append(connUrl)
                .append("' **\n");
            log.logInfo(logMessage);
        }
    }

    public static void resetMetrics() {
        clusterMetrics.clear();
        instanceMetrics.clear();
    }
}
