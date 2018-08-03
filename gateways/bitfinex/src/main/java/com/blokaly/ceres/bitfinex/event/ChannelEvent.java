package com.blokaly.ceres.bitfinex.event;

public abstract class ChannelEvent extends AbstractEvent {
  public static final String ORDERBOOK_CHANNEL = "book";
  public static final String TRADE_CHANNEL = "trades";
  public static final ChannelEvent UNKNOWN_EVENT = new UnknownChannelEvent();
  public final int channelId;

  public ChannelEvent(int channelId, String type) {
    super(type);
    this.channelId = channelId;
  }

  public int getChannelId() {
    return channelId;
  }

  private static class UnknownChannelEvent extends ChannelEvent {
    private UnknownChannelEvent() {
      super(0, "");
    }
  }
}
