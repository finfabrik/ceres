package com.blokaly.ceres.bitfinex.event;

public class SubscriptionEvent extends AbstractEvent {
  private String channel;
  private int chanId;
  private String prec;
  private String freq;
  private String len;
  private String pair;

  public String getChannel() {
    return channel;
  }

  public int getChanId() {
    return chanId;
  }

  public String getPair() {
    return pair;
  }

  @Override
  public String toString() {
    return "SubscriptionEvent{" +
        "channel=" + channel +
        ", chanId=" + chanId +
        ", prec=" + prec +
        ", freq=" + freq +
        ", len=" + len +
        ", pair=" + pair +
        '}';
  }
}
