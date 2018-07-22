package com.blokaly.ceres.cryptocompare.api;

public class MinuteBars {
  private final boolean success;
  private final String message;
  private final long timeFrom;
  private final long timeTo;
  private final Bar[] bars;

  private MinuteBars(boolean success, String message, long timeFrom, long timeTo, Bar... bars) {
    this.success = success;
    this.message = message;
    this.timeFrom = timeFrom;
    this.timeTo = timeTo;
    this.bars = bars;
  }

  public static MinuteBars success(long from, long to, Bar... bars) {
    return new MinuteBars(true, null, from, to, bars);
  }

  public static MinuteBars fail(String message) {
    return new MinuteBars(false, message, 0, 0);
  }

  public static class Bar {
    private long time;
    private double open;
    private double high;
    private double low;
    private double close;
    private double volumefrom;
    private double volumeto;
  }
}
