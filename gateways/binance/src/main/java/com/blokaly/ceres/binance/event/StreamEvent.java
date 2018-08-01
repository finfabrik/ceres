package com.blokaly.ceres.binance.event;

import com.google.gson.JsonObject;

public class StreamEvent {
  public static final String DEPTH_STREAM = "@depth";
  public static final String TRADE_STREAM = "@trade";

  private String stream;
  private JsonObject data;

  public String getStream() {
    return stream;
  }

  public JsonObject getData() {
    return data;
  }
}
