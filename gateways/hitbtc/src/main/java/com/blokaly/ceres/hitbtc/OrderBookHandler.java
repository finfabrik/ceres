package com.blokaly.ceres.hitbtc;

import com.blokaly.ceres.common.PairSymbol;
import com.blokaly.ceres.common.Source;
import com.blokaly.ceres.hitbtc.event.OrderBookRequestEvent;
import com.blokaly.ceres.orderbook.PriceBasedOrderBook;
import com.blokaly.ceres.system.CommonConfigs;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

@Singleton
public class OrderBookHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(OrderBookHandler.class);
  private final Map<String, PriceBasedOrderBook> orderbooks;
  private final Map<Long, String> subMap;

  @Inject
  public OrderBookHandler(Config config, @Named("SymbolMap") Map<String, PairSymbol> symbols) {
    String source = Source.valueOf(config.getString(CommonConfigs.APP_SOURCE).toUpperCase()).getCode();
    orderbooks = symbols.values().stream()
        .collect(Collectors.toMap(pair -> pair.getCode().toUpperCase(), pair -> {
          String key = pair.getCode() + "." + source;
          return new PriceBasedOrderBook(pair.getCode(), key);
        }));
    subMap = Maps.newConcurrentMap();
  }

  public PriceBasedOrderBook get(String symbol) {
    return orderbooks.get(symbol);
  }

  public Collection<String> getAllChannels() {
    return orderbooks.keySet();
  }

  public Collection<PriceBasedOrderBook> getAllBooks() {
    return orderbooks.values();
  }

  public void resetOrderBook(long subId) {
    if (subMap.containsKey(subId)) {
      orderbooks.get(subMap.get(subId)).clear();
    }
  }

  public void publishOpen() {

  }

  public void subscribeAll(HitbtcClient sender, Gson gson) {
    subMap.clear();
    orderbooks.keySet().forEach(sym -> {
      long id = System.nanoTime();
      subMap.put(id, sym);
      try {
        String jsonString = gson.toJson(new OrderBookRequestEvent(sym, id));
        LOGGER.info("Subscribing to order book[{}]...", sym);
        sender.send(jsonString);
      } catch (Exception e) {
        LOGGER.error("Error subscribing to order book " + sym, e);
      }
    });
  }
}