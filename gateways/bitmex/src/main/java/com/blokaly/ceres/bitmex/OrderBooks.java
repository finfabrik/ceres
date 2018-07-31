package com.blokaly.ceres.bitmex;

import com.blokaly.ceres.orderbook.OrderBasedOrderBook;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.Collection;
import java.util.Map;

@Singleton
public class OrderBooks {

  private final Map<String, OrderBasedOrderBook> orderbooks;

  @Inject
  public OrderBooks(Map<String, OrderBasedOrderBook> orderbooks) {
    this.orderbooks = orderbooks;
  }

  public OrderBasedOrderBook get(String symbol) {
    return orderbooks.get(symbol);
  }

  public Collection<String> getAllSymbols() {
    return orderbooks.keySet();
  }

  public Collection<OrderBasedOrderBook> getAllBooks() {
    return orderbooks.values();
  }

  public void clearAllBooks() {
    orderbooks.values().forEach(OrderBasedOrderBook::clear);
  }
}
