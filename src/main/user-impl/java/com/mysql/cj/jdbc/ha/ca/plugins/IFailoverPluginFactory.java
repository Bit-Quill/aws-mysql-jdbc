package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.ha.ca.ClusterAwareConnectionProxy;
import com.mysql.cj.log.Log;

public interface IFailoverPluginFactory {
  IFailoverPlugin getInstance(ClusterAwareConnectionProxy proxy, PropertySet propertySet, IFailoverPlugin next, Log log);
}
