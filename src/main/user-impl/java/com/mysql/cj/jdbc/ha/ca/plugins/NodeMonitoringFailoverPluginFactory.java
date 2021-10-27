package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;

import java.sql.Connection;

public class NodeMonitoringFailoverPluginFactory implements IFailoverPluginFactory {
  @Override
  public IFailoverPlugin getInstance(
      Connection connection,
      PropertySet propertySet,
      HostInfo hostInfo,
      IFailoverPlugin next,
      Log log) {
    return new NodeMonitoringFailoverPlugin(connection, propertySet, hostInfo, next, log);
  }
}
