package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.ha.ca.ClusterAwareConnectionProxy;
import com.mysql.cj.log.Log;

public class DefaultFailoverPluginFactory implements IFailoverPluginFactory {
  @Override
  public IFailoverPlugin getInstance(
      ClusterAwareConnectionProxy proxy,
      PropertySet propertySet,
      IFailoverPlugin next,
      Log log) {
    return new DefaultFailoverPlugin(log);
  }
}
