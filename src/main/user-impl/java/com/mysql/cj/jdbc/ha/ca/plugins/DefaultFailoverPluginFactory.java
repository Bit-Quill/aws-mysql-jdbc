package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;

import java.sql.Connection;

public class DefaultFailoverPluginFactory implements IFailoverPluginFactory {
  @Override
  public IFailoverPlugin getInstance(
      Connection connection,
      PropertySet propertySet,
      HostInfo hostInfo,
      IFailoverPlugin next, Log log) {
    final IFailoverPlugin plugin = new DefaultFailoverPlugin();
    plugin.init(connection, propertySet, hostInfo, next, log);
    return plugin;
  }
}
