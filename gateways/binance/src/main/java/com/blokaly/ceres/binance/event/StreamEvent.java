package com.blokaly.ceres.binance.event;

import com.google.gson.JsonObject;

public class StreamEvent {
  public static final String DEPTH_STREAM = "depth";
  public static final String TRADE_STREAM = "trade";
  public static final String STREAM_DELIMITER = "@";

  private String stream;
  private String sym;
  private String channel;
  private JsonObject data;

  public String getStream() {
    return stream;
  }

  public String getSymbol() {
    if (sym != null) {
      return sym;
    }
    if (stream == null) {
      return null;
    }
    parseStream();
    return sym;
  }

  public String getChannel() {
    if (channel != null) {
      return channel;
    }
    if (stream == null) {
      return null;
    }
    parseStream();
    return channel;
  }

  public JsonObject getData() {
    return data;
  }

  private void parseStream() {
    String[] symchan = stream.split(STREAM_DELIMITER);
    if (symchan.length == 2) {
      sym = symchan[0];
      channel = symchan[1];
    } else {
      sym = stream;
    }
  }
}
