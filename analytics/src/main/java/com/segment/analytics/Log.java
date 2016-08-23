package com.segment.analytics;

/** Abstraction for logging messages. */
public interface Log {
  enum Level {
    VERBOSE, DEBUG, ERROR, NONE
  }

  void print(String msg);

  /** A {@link Log} implementation which does nothing. */
  Log NONE = new Log() {
    @Override public void print(String msg) {

    }
  };

  /** A {@link Log} implementation which logs to {@link System#out}. */
  Log STDOUT = new Log() {
    @Override public void print(String msg) {
      System.out.println(msg);
    }
  };
}
