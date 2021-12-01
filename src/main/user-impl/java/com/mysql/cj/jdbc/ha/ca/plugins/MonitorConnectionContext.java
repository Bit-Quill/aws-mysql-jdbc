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

import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;

import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public class MonitorConnectionContext {
  private final int failureDetectionIntervalMillis;
  private final int failureDetectionTimeMillis;
  private final int failureDetectionCount;

  private final Set<String> nodeKeys;
  private final Log log;
  private final JdbcConnection connectionToAbort;

  private long startMonitorTime;
  private long invalidNodeStartTime;
  private int failureCount;
  private boolean nodeUnhealthy;
  private boolean activeContext = true;

  public MonitorConnectionContext(
      JdbcConnection connectionToAbort,
      Set<String> nodeKeys,
      Log log,
      int failureDetectionTimeMillis,
      int failureDetectionIntervalMillis,
      int failureDetectionCount) {
    this.connectionToAbort = connectionToAbort;
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

  void setInvalidNodeStartTime(long invalidNodeStartTimeMillis) {
    this.invalidNodeStartTime = invalidNodeStartTimeMillis;
  }

  void resetInvalidNodeStartTime() { this.invalidNodeStartTime = 0; }

  boolean isInvalidNodeStartTimeDefined() { return this.invalidNodeStartTime > 0; }

  public long getInvalidNodeStartTime() { return this.invalidNodeStartTime; }

  public boolean isNodeUnhealthy() {
    return this.nodeUnhealthy;
  }

  void setNodeUnhealthy(boolean nodeUnhealthy) { this.nodeUnhealthy = nodeUnhealthy; }

  public boolean isActiveContext() { return this.activeContext; }

  public void invalidate() { this.activeContext = false; }

  synchronized void abortConnection() {
    if (this.connectionToAbort == null || !this.activeContext) {
      return;
    }

    try {
      this.connectionToAbort.abortInternal();
    } catch (SQLException sqlEx) {
      // ignore
      this.log.logTrace(String.format("Exception during aborting connection: %s", sqlEx.getMessage()));
    }
  }

  public void updateConnectionStatus(long currentTime, boolean isValid, long validationIntervalTimeMillis) {
    if (!this.activeContext) {
      return;
    }

    final long totalElapsedTimeMillis = currentTime - this.startMonitorTime;

    if (totalElapsedTimeMillis > this.failureDetectionTimeMillis) {
      this.setConnectionValid(isValid, currentTime, validationIntervalTimeMillis);
    }
  }

  void setConnectionValid(boolean connectionValid, long currentTime, long validationIntervalTimeMillis) {
    if (!connectionValid) {
      this.failureCount++;

      if (!this.isInvalidNodeStartTimeDefined()) {
        this.setInvalidNodeStartTime(currentTime);
      }

      final long invalidNodeDurationMillis = currentTime - this.getInvalidNodeStartTime();
      final long maxInvalidNodeDurationMillis = (long) this.getFailureDetectionIntervalMillis() * this.getFailureDetectionCount();

      if (invalidNodeDurationMillis >= maxInvalidNodeDurationMillis) {
        this.log.logTrace(
            String.format(
                "[MonitorConnectionContext] node '%s' is *dead*.",
                nodeKeys));
        this.setNodeUnhealthy(true);
        this.abortConnection();
        return;
      }

      this.log.logTrace(String.format(
          "[MonitorConnectionContext] node '%s' is not *responding* (%d).",
          nodeKeys,
          this.getFailureCount()));
      return;
    }

    this.setFailureCount(0);
    this.resetInvalidNodeStartTime();
    this.setNodeUnhealthy(false);

    this.log.logTrace(
        String.format("[MonitorConnectionContext] node '%s' is *alive*.",
            nodeKeys));
  }
}
