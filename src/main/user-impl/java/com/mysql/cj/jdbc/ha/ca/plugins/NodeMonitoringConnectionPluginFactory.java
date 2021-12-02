package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;

public class NodeMonitoringConnectionPluginFactory implements IConnectionPluginFactory {
  @Override
  public IConnectionPlugin getInstance(
      ICurrentConnectionProvider currentConnectionProvider,
      PropertySet propertySet,
      IConnectionPlugin next,
      Log log) {
    return new NodeMonitoringConnectionPlugin(currentConnectionProvider, propertySet, next, log);
  }
}
