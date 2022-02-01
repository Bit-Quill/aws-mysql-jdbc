package com.mysql.cj.jdbc.ha.plugins.failover;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ha.plugins.ICurrentConnectionProvider;
import com.mysql.cj.log.Log;
import com.mysql.cj.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterAwareMetricsContainer implements IClusterAwareMetricsContainer {
    // ClusterID, Metrics
    private final static Map<String, ClusterAwareMetrics> clusterMetrics = new ConcurrentHashMap<>();
    // Instance URL, Metrics
    private final static Map<String, ClusterAwareMetrics> instanceMetrics = new ConcurrentHashMap<>();

    private ICurrentConnectionProvider currentConnectionProvider;
    private PropertySet propertySet;
    private String clusterID;

    public ClusterAwareMetricsContainer(ICurrentConnectionProvider currentConnectionProvider, PropertySet propertySet) {
        this.currentConnectionProvider = currentConnectionProvider;
        this.propertySet = propertySet;
    }

    public static void linkInstances(List<HostInfo> hosts, String clusterID) {
        final ClusterAwareMetrics metrics = clusterMetrics.computeIfAbsent(
            clusterID,
            k -> new ClusterAwareMetrics());
        for (HostInfo host : hosts) {
            clusterMetrics.put(host.getHost(), metrics);
        }
    }

    public void setClusterID(String clusterID) {
        this.clusterID = !StringUtils.isNullOrEmpty(clusterID) ? clusterID.substring(0, clusterID.indexOf(':'))
            : getCurrentConnUrl();
    }

    public void registerFailureDetectionTime(long timeMs) {
        if (!propertySet.getBooleanProperty(PropertyKey.gatherPerfMetrics.getKeyName()).getValue()) return;

        getClusterMetrics(clusterID).registerFailureDetectionTime(timeMs);

        if (propertySet.getBooleanProperty(PropertyKey.gatherAdditionalMetricsOnInstance.getKeyName()).getValue()) {
            getInstanceMetrics(getCurrentConnUrl()).registerFailureDetectionTime(timeMs);
        }
    }

    public void registerWriterFailoverProcedureTime(long timeMs) {
        if (!propertySet.getBooleanProperty(PropertyKey.gatherPerfMetrics.getKeyName()).getValue()) return;

        getClusterMetrics(clusterID).registerWriterFailoverProcedureTime(timeMs);

        if (propertySet.getBooleanProperty(PropertyKey.gatherAdditionalMetricsOnInstance.getKeyName()).getValue()) {
            getInstanceMetrics(getCurrentConnUrl()).registerWriterFailoverProcedureTime(timeMs);
        }
    }

    public void registerReaderFailoverProcedureTime(long timeMs) {
        if (!propertySet.getBooleanProperty(PropertyKey.gatherPerfMetrics.getKeyName()).getValue()) return;

        getClusterMetrics(clusterID).registerReaderFailoverProcedureTime(timeMs);

        if (propertySet.getBooleanProperty(PropertyKey.gatherAdditionalMetricsOnInstance.getKeyName()).getValue()) {
            getInstanceMetrics(getCurrentConnUrl()).registerReaderFailoverProcedureTime(timeMs);
        }
    }

    public void registerFailoverConnects(boolean isHit) {
        if (!propertySet.getBooleanProperty(PropertyKey.gatherPerfMetrics.getKeyName()).getValue()) return;

        getClusterMetrics(clusterID).registerFailoverConnects(isHit);

        if (propertySet.getBooleanProperty(PropertyKey.gatherAdditionalMetricsOnInstance.getKeyName()).getValue()) {
            getInstanceMetrics(getCurrentConnUrl()).registerFailoverConnects(isHit);
        }
    }

    public void registerInvalidInitialConnection(boolean isHit) {
        if (!propertySet.getBooleanProperty(PropertyKey.gatherPerfMetrics.getKeyName()).getValue()) return;

        getClusterMetrics(clusterID).registerInvalidInitialConnection(isHit);

        if (propertySet.getBooleanProperty(PropertyKey.gatherAdditionalMetricsOnInstance.getKeyName()).getValue()) {
            getInstanceMetrics(getCurrentConnUrl()).registerInvalidInitialConnection(isHit);
        }
    }

    public void registerUseLastConnectedReader(boolean isHit) {
        if (!propertySet.getBooleanProperty(PropertyKey.gatherPerfMetrics.getKeyName()).getValue()) return;

        getClusterMetrics(clusterID).registerUseLastConnectedReader(isHit);

        if (propertySet.getBooleanProperty(PropertyKey.gatherAdditionalMetricsOnInstance.getKeyName()).getValue()) {
            getInstanceMetrics(getCurrentConnUrl()).registerUseLastConnectedReader(isHit);
        }
    }

    public void registerUseCachedTopology(boolean isHit) {
        if (!propertySet.getBooleanProperty(PropertyKey.gatherPerfMetrics.getKeyName()).getValue()) return;

        getClusterMetrics(clusterID).registerUseCachedTopology(isHit);

        if (propertySet.getBooleanProperty(PropertyKey.gatherAdditionalMetricsOnInstance.getKeyName()).getValue()) {
            getInstanceMetrics(getCurrentConnUrl()).registerUseCachedTopology(isHit);
        }
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

            logMessage.append("** Performance Metrics Report for '");
            logMessage.append(connUrl);
            logMessage.append("' **\n");
            log.logInfo(logMessage);

            metrics.reportMetrics(log);
        } else {
            StringBuilder logMessage = new StringBuilder();
            logMessage.append("** No metrics collected for '");
            logMessage.append(connUrl);
            logMessage.append("' **\n");

            log.logInfo(logMessage);
        }
    }

    public static void resetMetrics() {
        clusterMetrics.clear();
        instanceMetrics.clear();
    }
}
