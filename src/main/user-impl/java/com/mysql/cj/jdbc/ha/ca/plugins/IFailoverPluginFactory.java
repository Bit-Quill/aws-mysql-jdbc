package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;

import java.sql.Connection;

public interface IFailoverPluginFactory {
  IFailoverPlugin getInstance(Connection connection, PropertySet propertySet, HostInfo hostInfo, IFailoverPlugin next, Log log);
}