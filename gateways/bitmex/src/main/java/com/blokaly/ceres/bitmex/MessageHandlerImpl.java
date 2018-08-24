package com.blokaly.ceres.bitmex;

import com.blokaly.ceres.bitmex.event.*;
import com.blokaly.ceres.common.DecimalNumber;
import com.blokaly.ceres.common.Pair;
import com.blokaly.ceres.orderbook.OrderBasedOrderBook;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

@Singleton
public class MessageHandlerImpl implements MessageHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageHandlerImpl.class);
  private final Gson gson;
  private final Provider<BitmexClient> clientProvider;
  private final OrderBooks orderBooks;

  @Inject
  public MessageHandlerImpl(Gson gson, Provider<BitmexClient> clientProvider, OrderBooks orderBooks) {
    this.gson = gson;
    this.clientProvider = clientProvider;
    this.orderBooks = orderBooks;
  }

  @Override
  public void onMessage(Open open) {
    LOGGER.info("WS session open");
    orderBooks.clearAllBooks();

    Collection<String> symbols = orderBooks.getAllSymbols();
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
    OrderBasedOrderBook book = orderBooks.get(snapshot.getSymbol());
    if (book == null) {
      return;
    }

    synchronized (book) {
      book.processSnapshot(snapshot);
      orderBooks.publishBook(book);
    }
  }

  @Override
  public void onMessage(Incremental incremental) {
    OrderBasedOrderBook book = orderBooks.get(incremental.getSymbol());
    if (book == null) {
      return;
    }

    synchronized (book) {
      book.processIncrementalUpdate(incremental);
      orderBooks.publishDelta(book);
    }
  }
}
