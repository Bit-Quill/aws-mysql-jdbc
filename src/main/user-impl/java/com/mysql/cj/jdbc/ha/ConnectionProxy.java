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

package com.mysql.cj.jdbc.ha;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.JdbcPropertySetImpl;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.jdbc.ha.plugins.BasicConnectionProvider;
import com.mysql.cj.jdbc.ha.plugins.ConnectionPluginManager;
import com.mysql.cj.jdbc.ha.plugins.ConnectionProvider;
import com.mysql.cj.jdbc.ha.plugins.ICurrentConnectionProvider;
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
public class ConnectionProxy implements ICurrentConnectionProvider, InvocationHandler {

  /** Null logger shared by all connections at startup. */
  protected static final Log NULL_LOGGER = new NullLogger(Log.LOGGER_INSTANCE_NAME);
  static final String METHOD_EQUALS = "equals";
  private static final String METHOD_HASH_CODE = "hashCode";
  private final JdbcPropertySetImpl connProps = new JdbcPropertySetImpl();
  /** The logger we're going to use. */
  protected transient Log log = NULL_LOGGER;
  // writer host is always stored at index 0
  protected Map<String, String> initialConnectionProps;
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
   * @param connection {@link JdbcConnection}
   * @throws SQLException if an error occurs
   */
  public ConnectionProxy(ConnectionUrl connectionUrl, JdbcConnection connection) throws SQLException {
    this.currentHostInfo = connectionUrl.getMainHost();
    this.currentConnection = connection;

    initLogger(connectionUrl);
    initSettings(connectionUrl);
    initProxy(ConnectionPluginManager::new);

    this.currentConnection.setConnectionLifecycleInterceptor(new ConnectionProxyLifecycleInterceptor(this.pluginManager));
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

    ConnectionProvider connectionProvider = new BasicConnectionProvider();
    final ConnectionProxy connProxy = new ConnectionProxy(connectionUrl, connectionProvider.connect(connectionUrl.getMainHost()));
    if (connProxy.isPluginEnabled()) {
      return (JdbcConnection)
          java.lang.reflect.Proxy.newProxyInstance(
              JdbcConnection.class.getClassLoader(),
              new Class<?>[] {JdbcConnection.class},
              connProxy);
    }

    // If plugin system is disabled, reset proxy settings from the connection.
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
    ConnectionProvider connectionProvider = new BasicConnectionProvider();
    final ConnectionProxy connProxy = new ConnectionProxy(connectionUrl, connectionProvider.connect(connectionUrl.getMainHost()));

    return (JdbcConnection)
        java.lang.reflect.Proxy.newProxyInstance(
            JdbcConnection.class.getClassLoader(),
            new Class<?>[] {JdbcConnection.class},
            connProxy);
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
    try {
      if (this.currentConnection != null && !this.currentConnection.isClosed()) {
        this.currentConnection.close();
      }
    } catch (SQLException sqlEx) {
      // ignore
    }

    this.currentConnection = connection;
    this.currentHostInfo = info;
  }

  @Override
  public synchronized Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
    final String methodName = method.getName();

    if (isDirectExecute(methodName, args)) {
      return executeMethodDirectly(methodName, args);
    }

    try {
      Object result = this.pluginManager.execute(
          this.currentConnection.getClass(),
          methodName,
          () -> method.invoke(currentConnection, args));
      return proxyIfReturnTypeIsJdbcInterface(method.getReturnType(), result);
    } catch (Exception e) {
      // Check if the captured exception must be wrapped by an unchecked exception.
      Class<?>[] declaredExceptions = method.getExceptionTypes();
      for (Class<?> declaredException : declaredExceptions) {
        if (declaredException.isAssignableFrom(e.getClass())) {
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

  protected void initSettings(ConnectionUrl connectionUrl) throws SQLException {
    try {
      this.connProps.initializeProperties(connectionUrl.getConnectionArgumentsAsProperties());
      this.pluginsEnabled =
          this.connProps.getBooleanProperty(PropertyKey.useConnectionPlugins).getValue();
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
        final Class<?> toProxyClass = toProxy.getClass();
        return Proxy.newProxyInstance(
            toProxyClass.getClassLoader(),
            Util.getImplementedInterfaces(toProxyClass),
            getNewJdbcInterfaceProxy(toProxy));
      }
    }
    return toProxy;
  }

  /**
   * Special handling of method calls that can be handled without making an explicit invocation against the connection
   * underlying this proxy. See {@link #isDirectExecute(String, Object[])}
   *
   * @param methodName The name of the method being called
   * @param args The argument parameters of the method that is being called
   * @return The results of the special method handling, according to which method was called
   */
  private Object executeMethodDirectly(String methodName, Object[] args) {
    if (METHOD_EQUALS.equals(methodName)) {
      return args[0].equals(this);
    }

    if (METHOD_HASH_CODE.equals(methodName)) {
      return this.hashCode();
    }

    // should never reach this statement, as the conditions in this method were previously checked in the method
    // calling this class using the isForwardingRequired method
    return null;
  }

  protected void initProxy(Function<Log, ConnectionPluginManager> connectionPluginManagerInitializer) {
    if (this.pluginManager == null) {
      this.pluginManager = connectionPluginManagerInitializer.apply(log);
      this.pluginManager.init(this, connProps);
    }
  }

  /**
   * Check if the method that is about to be invoked requires forwarding to the connection underlying this proxy. The
   * methods indicated below can be handled without needing to perform an invocation against the underlying connection,
   * provided the arguments are valid when required (eg for METHOD_EQUALS and METHOD_ABORT)
   *
   * @param methodName The name of the method that is being called
   * @param args The argument parameters of the method that is being called
   * @return true if we need to explicitly invoke the method indicated by methodName on the underlying connection
   */
  private boolean isDirectExecute(String methodName, Object[] args) {

    return (METHOD_EQUALS.equals(methodName)
        || METHOD_HASH_CODE.equals(methodName)
        && (args == null || args.length <= 0 || args[0] == null));
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

    public synchronized Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (METHOD_EQUALS.equals(method.getName())) {
        // Let args[0] "unwrap" to its InvocationHandler if it is a proxy.
        return args[0].equals(this);
      }

      synchronized(ConnectionProxy.this) {
        Object result =
          ConnectionProxy.this.pluginManager.execute(
            this.invokeOn.getClass(),
            method.getName(),
            () -> method.invoke(this.invokeOn, args));
        return proxyIfReturnTypeIsJdbcInterface(method.getReturnType(), result);
      }
    }
  }
}
