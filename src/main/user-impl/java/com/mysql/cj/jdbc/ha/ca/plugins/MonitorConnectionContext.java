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

import com.mysql.cj.log.Log;

import java.util.Set;

public class MonitorConnectionContext {
  private final int failureDetectionIntervalMillis;
  private final int failureDetectionTimeMillis;
  private final int failureDetectionCount;

  private final Set<String> nodeKeys;
  private final Log log;

  private long startMonitorTime;
  private int failureCount;
  private boolean isNodeUnhealthy;

  public MonitorConnectionContext(
      Set<String> nodeKeys,
      Log log,
      int failureDetectionTimeMillis,
      int failureDetectionIntervalMillis,
      int failureDetectionCount) {
    this.nodeKeys = nodeKeys;
    this.log = log;
    this.failureDetectionTimeMillis = failureDetectionTimeMillis;
    this.failureDetectionIntervalMillis = failureDetectionIntervalMillis;
    this.failureDetectionCount = failureDetectionCount;
  }

  void setStartMonitorTime(long startMonitorTime) {
    this.startMonitorTime = startMonitorTime;
  }

  Set<String> getNodeKeys() {
    return this.nodeKeys;
  }

  public int getFailureDetectionTimeMillis() {
    return failureDetectionTimeMillis;
  }

  public int getFailureDetectionIntervalMillis() {
    return failureDetectionIntervalMillis;
  }

  public int getFailureDetectionCount() {
    return failureDetectionCount;
  }

  public int getFailureCount() {
    return this.failureCount;
  }

  void setFailureCount(int failureCount) {
    this.failureCount = failureCount;
  }

  public boolean isNodeUnhealthy() {
    return this.isNodeUnhealthy;
  }

  void updateConnectionStatus(long currentTime, boolean isValid, long connectionValidationElapsedTime) {
    final long totalElapsedTimeMillis = currentTime - this.startMonitorTime;

    if (totalElapsedTimeMillis > this.failureDetectionTimeMillis) {
      this.setConnectionValid(isValid && (this.failureDetectionIntervalMillis >= connectionValidationElapsedTime));
    }
  }

  void setConnectionValid(boolean connectionValid) {
    if (!connectionValid) {
      this.failureCount++;

      if (this.getFailureCount() >= this.getFailureDetectionCount()) {
        this.log.logTrace(
            String.format(
                "[MonitorConnectionContext] node '%s' is *dead*.",
                nodeKeys));
        isNodeUnhealthy = true;
        return;
      }
      this.log.logTrace(String.format(
          "[MonitorConnectionContext] node '%s' is not *responding* (%d).",
          nodeKeys,
          this.getFailureCount()));
    } else {
      this.setFailureCount(0);
    }

    this.log.logTrace(
        String.format("[NodeMonitoringFailoverPlugin::Monitor] node '%s' is *alive*.",
            nodeKeys));
    isNodeUnhealthy = false;
  }
}
