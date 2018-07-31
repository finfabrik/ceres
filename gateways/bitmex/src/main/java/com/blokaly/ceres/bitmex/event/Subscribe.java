package com.blokaly.ceres.bitmex.event;

import java.util.Collection;

public class Subscribe {
  private static final String orderbook = "orderBookL2:";
  private final String op = "subscribe";
  private final String[] args;

  public Subscribe(String... symbols) {
    args = new String[symbols.length];
    for (int i = 0; i < symbols.length; i++) {
      args[i] = orderbook + symbols[i].toUpperCase();
    }
  }

  public Subscribe(Collection<String> symbols) {
    args = new String[symbols.size()];
    int idx = 0;
    for (String symbol : symbols) {
      args[idx++] = orderbook + symbol.toUpperCase();
    }
  }
}
