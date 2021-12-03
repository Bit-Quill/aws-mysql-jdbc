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

package demo.customplugins;

import com.mysql.cj.jdbc.ha.ca.plugins.IConnectionPlugin;
import com.mysql.cj.log.Log;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * This connection plugin tracks the execution time of all methods executed with
 * enhanced instance monitoring enabled.
 * During the cleanup phase when {@link ExecutionTimeConnectionPlugin#releaseResources()}
 * is called, this plugin logs all the methods executed and time spent on each execution
 * in milliseconds.
 */
public class ExecutionTimeConnectionPlugin implements IConnectionPlugin {
  final long initializeTime;
  final IConnectionPlugin nextPlugin;
  private final Log logger;
  private final Map<String, Long> results = new HashMap<>();

  public ExecutionTimeConnectionPlugin(
      IConnectionPlugin nextPlugin,
      Log logger) {
    this.nextPlugin = nextPlugin;
    this.logger = logger;

    initializeTime = System.currentTimeMillis();
  }

  @Override
  public Object execute(
      Class<?> methodInvokeOn,
      String methodName,
      Callable<?> executeSqlFunc)
      throws Exception {
    // This `execute` measures the time it takes for the remaining connection plugins to
    // execute the given method call.
    // In this sample, the next connection plugin is the `NodeMonitoringConnectionPlugin`,
    // so this measurement includes the time it takes for the
    // `NodeMonitoringConnectionPlugin` to finish all its necessary setups.
    final long startTime = System.nanoTime();
    final Object result =
        this.nextPlugin.execute(methodInvokeOn, methodName, executeSqlFunc);
    final long elapsedTime = System.nanoTime() - startTime;
    results.merge(
        methodName,
        elapsedTime / 1000000,
        Long::sum);

    return result;
  }

  @Override
  public void releaseResources() {
    // Output the aggregated information from all methods called throughout the lifespan
    // of the current connection.
    final long elapsedTime = System.nanoTime() - initializeTime;
    final String leftAlignFormat = "| %-19s | %-10s |\n";
    final StringBuilder logMessage = new StringBuilder();

    logMessage.append("** ExecutionTimeConnectionPlugin Summary **\n");
    logMessage.append(String.format(
        "Plugin Uptime: %ds\n",
        elapsedTime / 1000000
    ));

    logMessage
        .append("** Method Execution Time With Enhanced Instance Monitoring **\n")
        .append("+---------------------+------------+\n")
        .append("| Method Executed     | Total Time |\n")
        .append("+---------------------+------------+\n");

    results.forEach((key, val) -> logMessage.append(String.format(
        leftAlignFormat,
        key,
        val + "ms")));
    logMessage.append("+---------------------+------------+\n");
    logger.logInfo(logMessage);

    results.clear();

    // Traverse the connection plugin chain by calling the next plugin. This step allows
    // all connection plugins a chance to clean up any dangling resources or perform any
    // last tasks before shutting down.
    // In this sample, `NodeMonitoringConnectionPlugin#releaseResources()` will be called
    // to release any running monitoring threads.
    this.nextPlugin.releaseResources();
  }
}
