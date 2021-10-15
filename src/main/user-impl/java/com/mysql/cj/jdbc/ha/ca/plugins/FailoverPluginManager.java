/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;
import org.jboss.util.NullArgumentException;

import java.sql.Connection;
import java.util.concurrent.Callable;

//TODO: rename class name to more generic one. This plugin manager has nothing to do with failover so the current class name is confusing and misleading.
public class FailoverPluginManager {

  /* THIS CLASS IS NOT MULTI-THREADING SAFE */
  /* IT'S EXPECTED TO HAVE ONE INSTANCE OF THIS MANAGER PER JDBC CONNECTION */

  protected static final String DEFAULT_PLUGIN_FACTORIES =
      NodeMonitoringFailoverPluginFactory.class.getName();

  protected Log log;
  protected Connection connection = null;
  protected PropertySet propertySet = null;
  protected HostInfo hostInfo;
  protected IFailoverPlugin headPlugin = null;

  public FailoverPluginManager(Log log) {
    if (log == null) {
      throw new NullArgumentException("log");
    }

    this.log = log;
  }

  public void init(Connection connection, PropertySet propertySet, HostInfo hostInfo) {
    this.connection = connection;
    this.propertySet = propertySet;
    this.hostInfo = hostInfo;

    String factoryClazzNames = propertySet
        .getStringProperty(PropertyKey.failoverPluginsFactories)
        .getValue();

    if (StringUtils.isNullOrEmpty(factoryClazzNames)) {
      factoryClazzNames = DEFAULT_PLUGIN_FACTORIES;
    }

    this.headPlugin = new DefaultFailoverPluginFactory()
        .getInstance(
            this.connection,
            this.propertySet,
            this.hostInfo,
            null,
            this.log);

    if (!StringUtils.isNullOrEmpty(factoryClazzNames)) {
      IFailoverPluginFactory[] factories =
          Util.<IFailoverPluginFactory>loadClasses(
                  factoryClazzNames,
                  "MysqlIo.BadFailoverPluginFactory",
                  null)
              .toArray(new IFailoverPluginFactory[0]);

      // make a chain of analyzers with default one at the tail

      for (int i = factories.length - 1; i >= 0; i--) {
        this.headPlugin = factories[i]
            .getInstance(
                this.connection,
                this.propertySet,
                this.hostInfo,
                this.headPlugin,
                this.log);
      }
    }

  }

  //TODO: Should methodName contain not just method name but also an interface name? Like:
  // "execute" -> "JdbcConnection.execute"
  // For example method close() exists for Connection, Statement, ResultSet and Closeable interfaces. Thus it might be challenging for a plugin to identify what
  // exact method is actually been called.

  //TODO: Should method target to be passed along with method name? Like:
  // Object execute(Object methodTarget, String methodName, Callable executeSqlFunc)
  public Object execute(String methodName, Callable executeSqlFunc) throws Exception {
    return this.headPlugin.execute(methodName, executeSqlFunc);
  }

  public void releaseResources() {
    this.log.logTrace("[FailoverPluginManager.releaseResources]");
    this.headPlugin.releaseResources();
  }
}