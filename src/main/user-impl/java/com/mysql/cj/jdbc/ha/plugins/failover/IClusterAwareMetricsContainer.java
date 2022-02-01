package com.mysql.cj.jdbc.ha.plugins.failover;

public interface IClusterAwareMetricsContainer {
    void setClusterID(String clusterID);
    void registerFailureDetectionTime(long timeMs);
    void registerWriterFailoverProcedureTime(long timeMs);
    void registerReaderFailoverProcedureTime(long timeMs);
    void registerFailoverConnects(boolean isHit);
    void registerInvalidInitialConnection(boolean isHit);
    void registerUseLastConnectedReader(boolean isHit);
    void registerUseCachedTopology(boolean isHit);
}
