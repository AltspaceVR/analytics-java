package com.segment.analytics.internal;

import com.segment.analytics.Log;

public class Logger {
  final Log log;
  final Log.Level logLevel;

  public Logger(Log log, Log.Level logLevel) {
    this.log = log;
    this.logLevel = logLevel;
  }

  public void log(Log.Level logLevel, String format, Object... args) {
    if (this.logLevel.ordinal() <= logLevel.ordinal()) {
      log.print(String.format(format, args));
    }
  }
}
