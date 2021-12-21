/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of this program hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of this connector, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.jdbc.ha.ca;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.JdbcPropertySetImpl;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.jdbc.ha.ca.plugins.ConnectionPluginManager;
import com.mysql.cj.jdbc.ha.ca.plugins.ICurrentConnectionProvider;
import com.mysql.cj.jdbc.interceptors.ConnectionLifecycleInterceptor;
import com.mysql.cj.jdbc.interceptors.ConnectionLifecycleInterceptorProvider;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.LogFactory;
import com.mysql.cj.log.NullLogger;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Function;

/**
 * A proxy for a dynamic com.mysql.cj.jdbc.JdbcConnection implementation that provides cluster-aware
 * failover features. Connection switching occurs on communications related exceptions and/or
 * cluster topology changes.
 */
public class ClusterAwareConnectionProxy
    implements ConnectionLifecycleInterceptorProvider,
    ICurrentConnectionProvider,
    InvocationHandler {

  /** Null logger shared by all connections at startup. */
  protected static final Log NULL_LOGGER = new NullLogger(Log.LOGGER_INSTANCE_NAME);
  static final String METHOD_EQUALS = "equals";
  private static final String METHOD_HASH_CODE = "hashCode";
  private final JdbcPropertySetImpl connProps = new JdbcPropertySetImpl();
  /** The logger we're going to use. */
  protected transient Log log = NULL_LOGGER;
  // writer host is always stored at index 0
  protected Map<String, String> initialConnectionProps;
  protected boolean inTransaction = false;
  protected ConnectionProvider connectionProvider;
  protected ConnectionPluginManager pluginManager = null;
  // Configuration settings
  protected boolean pluginsEnabled = true;
  private HostInfo currentHostInfo;
  private JdbcConnection currentConnection;

  /**
   * Instantiates a new AuroraConnectionProxy for the given list of hosts and connection properties.
   *
   * @param connectionUrl {@link ConnectionUrl} instance containing the lists of hosts available to
   *     switch on.
   * @throws SQLException if an error occurs
   */
  public ClusterAwareConnectionProxy(ConnectionUrl connectionUrl) throws SQLException {
    this.currentHostInfo = connectionUrl.getMainHost();

    initSettings(connectionUrl);
    initLogger(connectionUrl);

    this.connectionProvider = new BasicConnectionProvider();

    initProxy(connectionUrl);
  }

  /**
   * Checks if connection is associated with Aurora cluster and instantiates a new
   * AuroraConnectionProxy if needed. Otherwise it returns a single-host connection.
   *
   * @param connectionUrl {@link ConnectionUrl} instance containing the lists of hosts available to
   *     switch on.
   * @throws SQLException if an error occurs
   */
  public static JdbcConnection autodetectClusterAndCreateProxyInstance(ConnectionUrl connectionUrl)
      throws SQLException {

    ClusterAwareConnectionProxy connProxy =
        new ClusterAwareConnectionProxy(connectionUrl);
    if (connProxy.isPluginEnabled()) {
      return (JdbcConnection)
          java.lang.reflect.Proxy.newProxyInstance(
              JdbcConnection.class.getClassLoader(),
              new Class<?>[] {JdbcConnection.class},
              connProxy);
    }

    // If failover is disabled, reset proxy settings from the connection.
    connProxy.currentConnection.setProxy(null);
    return connProxy.currentConnection;
  }

  /**
   * Instantiates a new AuroraConnectionProxy.
   *
   * @param connectionUrl {@link ConnectionUrl} instance containing the lists of hosts available to
   *     switch on.
   * @throws SQLException if an error occurs
   */
  public static JdbcConnection createProxyInstance(ConnectionUrl connectionUrl)
      throws SQLException {
    ClusterAwareConnectionProxy connProxy =
        new ClusterAwareConnectionProxy(connectionUrl);

    return (JdbcConnection)
        java.lang.reflect.Proxy.newProxyInstance(
            JdbcConnection.class.getClassLoader(),
            new Class<?>[] {JdbcConnection.class},
            connProxy);
  }

  @Override
  public ConnectionLifecycleInterceptor getConnectionLifecycleInterceptor() {
    return new ClusterAwareConnectionLifecycleInterceptor(this);
  }

  @Override
  public JdbcConnection getCurrentConnection() {
    return this.currentConnection;
  }

  @Override
  public HostInfo getCurrentHostInfo() {
    return this.currentHostInfo;
  }

  @Override
  public void setCurrentConnection(JdbcConnection connection, HostInfo info) {
    this.currentConnection = connection;
    this.currentHostInfo = info;
  }

  @Override
  public synchronized Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
    String methodName = method.getName();

    if (METHOD_EQUALS.equals(methodName)) {
      // Let args[0] "unwrap" to its InvocationHandler if it is a proxy.
      return args[0].equals(this);
    }

    if (METHOD_HASH_CODE.equals(methodName)) {
      return this.hashCode();
    }

    try {
      Object result = this.pluginManager.execute(
          this.currentConnection.getClass(),
          methodName,
          () -> method.invoke(currentConnection, args));
      return proxyIfReturnTypeIsJdbcInterface(method.getReturnType(), result);
    } catch (Exception e) {
      // Check if the captured exception must be wrapped by an unchecked exception.
      Class<?>[] declaredException = method.getExceptionTypes();
      for (Class<?> declEx : declaredException) {
        if (declEx.isAssignableFrom(e.getClass())) {
          throw e;
        }
      }
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  protected JdbcConnection getConnection() {
    return this.currentConnection;
  }

  protected InvocationHandler getNewJdbcInterfaceProxy(Object toProxy) {
    return new JdbcInterfaceProxy(toProxy);
  }

  protected void initLogger(ConnectionUrl connUrl) {
    String loggerClassName =
        connUrl.getOriginalProperties().get(PropertyKey.logger.getKeyName());
    if (!StringUtils.isNullOrEmpty(loggerClassName)) {
      this.log = LogFactory.getLogger(loggerClassName, Log.LOGGER_INSTANCE_NAME);
    }
  }

  protected void initProxy(ConnectionUrl connUrl) throws SQLException {
    this.initProxy(connUrl, ConnectionPluginManager::new);
  }

  protected void initSettings(ConnectionUrl connectionUrl) throws SQLException {
    JdbcPropertySetImpl connProps = new JdbcPropertySetImpl();
    try {
      connProps.initializeProperties(connectionUrl.getMainHost().exposeAsProperties());
      this.pluginsEnabled =
          connProps.getBooleanProperty(PropertyKey.useConnectionPlugins).getValue();
    } catch (CJException e) {
      throw SQLExceptionsMapping.translateException(e, null);
    }
  }

  /**
   * If the given return type is or implements a JDBC interface, proxies the given object so that we can catch SQL errors and fire a connection switch.
   *
   * @param returnType
   *            The type the object instance to proxy is supposed to be.
   * @param toProxy
   *            The object instance to proxy.
   * @return
   *         The proxied object or the original one if it does not implement a JDBC interface.
   */
  protected Object proxyIfReturnTypeIsJdbcInterface(Class<?> returnType, Object toProxy) {
    if (toProxy != null) {
      if (Util.isJdbcInterface(returnType)) {
        Class<?> toProxyClass = toProxy.getClass();
        return Proxy.newProxyInstance(
            toProxyClass.getClassLoader(),
            Util.getImplementedInterfaces(toProxyClass),
            getNewJdbcInterfaceProxy(toProxy));
      }
    }
    return toProxy;
  }

  private void initProxy(
      ConnectionUrl connUrl,
      Function<Log, ConnectionPluginManager> connectionPluginManagerInitializer) {
    this.currentHostInfo = connUrl.getMainHost();

    if (this.pluginManager == null) {
      this.pluginManager = connectionPluginManagerInitializer.apply(log);
      this.pluginManager.init(
          this,
          connProps);
    }
  }

  private boolean isPluginEnabled() {
    return this.pluginsEnabled;
  }

  // TODO: review
  private void releasePluginManager() {
    if (this.pluginManager != null) {
      this.pluginManager.releaseResources();
      this.pluginManager = null;
    }
  }

  /**
   * Proxy class to intercept and deal with errors that may occur in any object bound to the current connection.
   */
  class JdbcInterfaceProxy implements InvocationHandler {
    Object invokeOn;

    JdbcInterfaceProxy(Object toInvokeOn) {
      this.invokeOn = toInvokeOn;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (METHOD_EQUALS.equals(method.getName())) {
        // Let args[0] "unwrap" to its InvocationHandler if it is a proxy.
        return args[0].equals(this);
      }

      Object result =
          ClusterAwareConnectionProxy.this.pluginManager.execute(
              this.invokeOn.getClass(),
              method.getName(),
              () -> method.invoke(this.invokeOn, args));
      result = proxyIfReturnTypeIsJdbcInterface(method.getReturnType(), result);
      return result;
    }
  }
}
