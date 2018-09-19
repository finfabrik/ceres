package com.blokaly.ceres.system;

import java.util.concurrent.TimeUnit;

public class JavaClock implements CeresClock {

  @Override
  public long nanos() {
    return TimeUnit.MILLISECONDS.toNanos(millis());
  }

  @Override
  public long millis() {
    return System.currentTimeMillis();
  }
}
