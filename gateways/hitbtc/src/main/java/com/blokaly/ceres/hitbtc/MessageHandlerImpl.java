package com.blokaly.ceres.hitbtc;

import com.blokaly.ceres.hitbtc.data.OrderbookNotification;
import com.blokaly.ceres.hitbtc.data.OrderbookSnapshot;
import com.blokaly.ceres.hitbtc.data.Trades;
import com.blokaly.ceres.hitbtc.event.ErrorEvent;
import com.blokaly.ceres.hitbtc.event.SubscribedEvent;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class MessageHandlerImpl implements MessageHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageHandlerImpl.class);
  private final Gson gson;
  private final Provider<HitbtcClient> clientProvider;
  private final OrderBookHandler orderBookHandler;
  private final TradesHandler tradesHandler;

  @Inject
  public MessageHandlerImpl(Gson gson,
                            Provider<HitbtcClient> clientProvider,
                            OrderBookHandler orderBookHandler,
                            TradesHandler tradesHandler
                            ) {
    this.gson = gson;
    this.clientProvider = clientProvider;
    this.orderBookHandler = orderBookHandler;
    this.tradesHandler = tradesHandler;

  }

  @Override
  public void onMessage(SubscribedEvent event) {
    if (event.getResult()) {
      LOGGER.info("Subscription success: {}", event);
      long subId = event.getSubId();
      orderBookHandler.resetOrderBook(subId);
    } else {
      LOGGER.error("Subscription failed: {}", event);
    }
  }

  @Override
  public void onMessage(OrderbookSnapshot snapshot) {
    orderBookHandler.processSnapshot(snapshot);
  }

  @Override
  public void onMessage(OrderbookNotification update) {
    orderBookHandler.processIncremental(update);
  }

  @Override
  public void onMessage(Trades event) {
    tradesHandler.publishTrades(event);
  }

  @Override
  public void onMessage(ErrorEvent event) {
    LOGGER.error("Exchange Response from exchange : " + event);
  }

  @Override
  public void subscribeAllPairs() {
    HitbtcClient sender = clientProvider.get();
    if (sender == null) {
      LOGGER.debug("Hitbtc client not initialized");
      return;
    }

    orderBookHandler.publishOpen();
    tradesHandler.publishOpen();
    orderBookHandler.subscribeAll(sender, gson);
    tradesHandler.subscribeAll(sender, gson);

  }
}
