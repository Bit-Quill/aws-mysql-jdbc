package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;

public interface IConnectionPluginFactory {
  IConnectionPlugin getInstance(ICurrentConnectionProvider currentConnectionProvider, PropertySet propertySet, IConnectionPlugin next, Log log);
}
