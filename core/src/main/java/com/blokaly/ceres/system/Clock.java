package com.blokaly.ceres.system;

public class Clock implements CeresClock {
  static {
    System.loadLibrary("nativetime");
  }

  private native long currentTimeInNanos();

  public long nanos() {
    return currentTimeInNanos();
  }

  public long millis() {
    return System.currentTimeMillis();
  }
}
