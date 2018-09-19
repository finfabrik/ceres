package com.blokaly.ceres.system;

public class Clock {
  static {
    System.loadLibrary("nativetime");
  }

  private native long currentTimeInNanos();

  public long nanoTime() {
    return currentTimeInNanos();
  }

}
