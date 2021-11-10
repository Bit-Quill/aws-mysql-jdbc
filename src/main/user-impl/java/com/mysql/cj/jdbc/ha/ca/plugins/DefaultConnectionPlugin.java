package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.log.Log;

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
  public Object execute(String methodName, Callable executeSqlFunc) throws Exception {
    this.log.logTrace(
        String.format("[DefaultConnectionPlugin.execute]: method=%s >>>>>", methodName));
    try {
      return executeSqlFunc.call();
    } catch (Exception ex) {
      this.log.logTrace(
          String.format("[DefaultConnectionPlugin.execute]: method=%s, exception: ", methodName), ex);
      throw ex;
    } finally {
      this.log.logTrace(
          String.format("[DefaultConnectionPlugin.execute]: method=%s <<<<<", methodName));
    }
  }

  @Override
  public void releaseResources() {
    // do nothing
  }
}
