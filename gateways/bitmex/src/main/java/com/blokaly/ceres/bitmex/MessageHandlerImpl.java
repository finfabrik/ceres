package com.blokaly.ceres.bitmex;

import com.blokaly.ceres.bitmex.event.*;
import com.blokaly.ceres.orderbook.OrderBasedOrderBook;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

@Singleton
public class MessageHandlerImpl implements MessageHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageHandlerImpl.class);
  private final Gson gson;
  private final Provider<BitmexClient> clientProvider;
  private final OrderBookHandler orderBookHandler;
  private final TradesHandler tradesHandler;

  @Inject
  public MessageHandlerImpl(Gson gson, Provider<BitmexClient> clientProvider, OrderBookHandler orderBookHandler, TradesHandler tradesHandler) {
    this.gson = gson;
    this.clientProvider = clientProvider;
    this.orderBookHandler = orderBookHandler;
    this.tradesHandler = tradesHandler;
  }

  @Override
  public void onMessage(Open open) {
    LOGGER.info("WS session open");
    orderBookHandler.clearAllBooks();
    orderBookHandler.publishOpen();
    tradesHandler.publieOpen();
    Collection<String> symbols = orderBookHandler.getAllSymbols();
    String jsonString = gson.toJson(new Subscribe(symbols));
    LOGGER.info("subscribing: {}", jsonString);
    clientProvider.get().send(jsonString);
  }

  @Override
  public void onMessage(Close close) {
    LOGGER.info("WS session close");
  }

  @Override
  public void onMessage(Subscription subscription) {
    LOGGER.info("{} subscription {}", subscription.getSubscribe(), subscription.isSuccess() ? "success" : "failed");
  }

  @Override
  public void onMessage(Snapshot snapshot) {
    orderBookHandler.processSnapshot(snapshot);
  }

  @Override
  public void onMessage(Incremental incremental) {
    orderBookHandler.processIncremental(incremental);
  }

  @Override
  public void onMessage(Trades trades) {
    Collection<Trades.Trade> tradeBag = trades.getTrades();
    tradesHandler.publishTrades(tradeBag);
  }
}
