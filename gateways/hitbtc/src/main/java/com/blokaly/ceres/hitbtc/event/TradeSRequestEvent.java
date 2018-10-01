package com.blokaly.ceres.hitbtc.event;

import java.util.HashMap;
import java.util.Map;

public class TradeSRequestEvent extends AbstractEvent{

  private final Map<String, String> params;
  private final long id;

  public TradeSRequestEvent(String pair, long id){
    super("subscribeTrades");
    params = new HashMap<>();
    params.put("symbol", pair);
    this.id = id;
  }
}
