package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.log.Log;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Callable;

public class DefaultConnectionPlugin implements IConnectionPlugin {

  protected Log log;

  public DefaultConnectionPlugin(Log log) {
    if (log == null) {
      throw new NullArgumentException("log");
    }

    this.log = log;
  }

  @Override
  public Object execute(Class<?> methodInvokeOn, String methodName, Callable<?> executeSqlFunc) throws Exception {
    this.log.logTrace(
        String.format("[DefaultConnectionPlugin.execute]: method=%s.%s >>>>>", methodInvokeOn.getName(), methodName));
    try {
      return executeSqlFunc.call();
    } catch (InvocationTargetException invocationTargetException) {
      this.log.logTrace(
              String.format("[DefaultConnectionPlugin.execute]: method=%s.%s, exception: ", methodInvokeOn.getName(), methodName), invocationTargetException);
      Throwable targetException = invocationTargetException.getTargetException();
      if (targetException instanceof Error) {
        throw (Error) targetException;
      }
      throw (Exception) targetException;
    } catch (Exception ex) {
      this.log.logTrace(
          String.format("[DefaultConnectionPlugin.execute]: method=%s.%s, exception: ", methodInvokeOn.getName(), methodName), ex);
      throw ex;
    } finally {
      this.log.logTrace(
          String.format("[DefaultConnectionPlugin.execute]: method=%s.%s <<<<<", methodInvokeOn.getName(), methodName));
    }
  }

  @Override
  public void releaseResources() {
    // do nothing
  }
}
