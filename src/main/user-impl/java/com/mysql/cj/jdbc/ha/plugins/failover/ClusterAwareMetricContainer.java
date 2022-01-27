package com.mysql.cj.jdbc.ha.plugins.failover;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterAwareMetricContainer {
    private static ClusterAwareMetricContainer singleton = null;
    private final Map<String, ClusterAwareMetrics> metricsMap = new ConcurrentHashMap<>();

    public static ClusterAwareMetricContainer getInstance() {
        if (singleton == null) {
            singleton = new ClusterAwareMetricContainer();
        }
        return singleton;
    }

    public ClusterAwareMetrics getOrCreate(String connUrl) {
        return getOrCreate(connUrl, true);
    }

    public ClusterAwareMetrics getOrCreate(String connUrl, boolean atClusterLevel) {
        if (StringUtils.isNullOrEmpty(connUrl)) {
            return null;
        }
        return metricsMap.computeIfAbsent(atClusterLevel ? connUrl + "-cluster" : connUrl, k -> new ClusterAwareMetrics());
    }

    public void populateMap(List<HostInfo> hosts, ClusterAwareMetrics metrics) {
        for (HostInfo hi : hosts) {
            final String host = hi.getHost() + "-cluster";
            metricsMap.put(host, metrics);
        }
    }
}
