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

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.ha.ca.ClusterAwareConnectionProxy;
import com.mysql.cj.log.Log;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;

public class ConnectionPluginManager {

  /* THIS CLASS IS NOT MULTI-THREADING SAFE */
  /* IT'S EXPECTED TO HAVE ONE INSTANCE OF THIS MANAGER PER JDBC CONNECTION */

  protected static final String DEFAULT_PLUGIN_FACTORIES =
      NodeMonitoringConnectionPluginFactory.class.getName();

  protected Log logger;
  protected PropertySet propertySet = null;
  protected IConnectionPlugin headPlugin = null;
  ClusterAwareConnectionProxy proxy;
  protected static final List<ConnectionPluginManager> instances = new CopyOnWriteArrayList<>();

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(ConnectionPluginManager::releaseAllResources));
  }

  public ConnectionPluginManager(Log logger) {
    if (logger == null) {
      throw new IllegalArgumentException(NullArgumentMessage.getMessage("logger"));
    }

    this.logger = logger;
  }

  public void init(ClusterAwareConnectionProxy proxy, PropertySet propertySet) {
    instances.add(this);
    this.proxy = proxy;
    this.propertySet = propertySet;

    String factoryClazzNames = propertySet
        .getStringProperty(PropertyKey.connectionPluginFactories)
        .getValue();

    if (StringUtils.isNullOrEmpty(factoryClazzNames)) {
      factoryClazzNames = DEFAULT_PLUGIN_FACTORIES;
    }

    this.headPlugin = new DefaultConnectionPluginFactory()
        .getInstance(
            this.proxy,
            this.propertySet,
            null,
            this.logger);

    if (!StringUtils.isNullOrEmpty(factoryClazzNames)) {
      IConnectionPluginFactory[] factories =
          Util.<IConnectionPluginFactory>loadClasses(
                  factoryClazzNames,
                  "MysqlIo.BadConnectionPluginFactory",
                  null)
              .toArray(new IConnectionPluginFactory[0]);

      // make a chain of analyzers with default one at the tail

      for (int i = factories.length - 1; i >= 0; i--) {
        this.headPlugin = factories[i]
            .getInstance(
                this.proxy,
                this.propertySet,
                this.headPlugin,
                this.logger);
      }
    }

  }

  public Object execute(Class<?> methodInvokeOn, String methodName, Callable<?> executeSqlFunc) throws Exception {
    return this.headPlugin.execute(methodInvokeOn, methodName, executeSqlFunc);
  }

  public void releaseResources() {
    instances.remove(this);
    this.logger.logTrace("[ConnectionPluginManager.releaseResources]");
    this.headPlugin.releaseResources();
  }

  public static void releaseAllResources() {
    for (ConnectionPluginManager instance : instances) {
      instance.releaseResources();
    }
  }
}
