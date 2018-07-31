package com.blokaly.ceres.bitmex.event;

import java.util.Collection;

public class Subscribe {
  private static final String orderbook = "orderBookL2:";
  private static final String liveTrade = "trade:";
  private final String op = "subscribe";
  private final String[] args;

  public Subscribe(Collection<String> symbols) {
    args = new String[2 * symbols.size()];
    int idx = 0;
    for (String symbol : symbols) {
      args[idx++] = orderbook + symbol.toUpperCase();
      args[idx++] = liveTrade + symbol.toUpperCase();
    }
  }
}
