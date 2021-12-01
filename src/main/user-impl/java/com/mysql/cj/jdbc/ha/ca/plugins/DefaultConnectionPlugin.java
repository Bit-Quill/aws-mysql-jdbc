package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.log.Log;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Callable;

public class DefaultConnectionPlugin implements IConnectionPlugin {

  protected Log logger;

  public DefaultConnectionPlugin(Log logger) {
    if (logger == null) {
      throw new IllegalArgumentException(NullArgumentMessage.getMessage("logger"));
    }

    this.logger = logger;
  }

  @Override
  public Object execute(Class<?> methodInvokeOn, String methodName, Callable<?> executeSqlFunc) throws Exception {
    try {
      return executeSqlFunc.call();
    } catch (InvocationTargetException invocationTargetException) {
      Throwable targetException = invocationTargetException.getTargetException();
      this.log.logTrace(
              String.format("[DefaultConnectionPlugin.execute]: method=%s.%s, exception: ", methodInvokeOn.getName(), methodName), targetException);
      if (targetException instanceof Error) {
        throw (Error) targetException;
      }
      throw (Exception) targetException;
    } catch (Exception ex) {
      this.log.logTrace(
          String.format("[DefaultConnectionPlugin.execute]: method=%s.%s, exception: ", methodInvokeOn.getName(), methodName), ex);
      throw ex;
    }
  }

  @Override
  public void releaseResources() {
    // do nothing
  }
}
