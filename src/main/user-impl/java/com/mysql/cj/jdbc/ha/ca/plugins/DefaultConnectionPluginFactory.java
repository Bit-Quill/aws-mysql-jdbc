package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.ha.ca.ClusterAwareConnectionProxy;
import com.mysql.cj.log.Log;

public class DefaultConnectionPluginFactory implements IConnectionPluginFactory {
  @Override
  public IConnectionPlugin getInstance(
      ClusterAwareConnectionProxy proxy,
      PropertySet propertySet,
      IConnectionPlugin next,
      Log log) {
    return new DefaultConnectionPlugin(log);
  }
}
