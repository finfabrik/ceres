package com.blokaly.ceres.hitbtc;

import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.hitbtc.data.OrderbookNotification;
import com.blokaly.ceres.hitbtc.data.OrderbookSnapshot;
import com.blokaly.ceres.hitbtc.data.Trades;
import com.blokaly.ceres.hitbtc.event.AbstractEvent;
import com.blokaly.ceres.hitbtc.event.ErrorEvent;
import com.blokaly.ceres.hitbtc.event.EventType;
import com.blokaly.ceres.hitbtc.event.SubscribedEvent;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

@Singleton
public class JsonCracker {

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonCracker.class);
  private final Gson gson;
  private final MessageHandler messageHandler;
  private final ExecutorService executor;

  @Inject
  JsonCracker(Gson gson, MessageHandler messageHandler, @SingleThread ExecutorService executor) {
    this.gson = gson;
    this.messageHandler = messageHandler;
    this.executor = executor;
  }

  void crack(String json) {
    AbstractEvent event = gson.fromJson(json, AbstractEvent.class);
    LOGGER.debug("event: {}", event);
    executor.execute(() -> process(event));
  }

  private void process(AbstractEvent event) {
    EventType eventType = EventType.get(event.getMethod());
    if (eventType == null) {
      return;
    }
    switch (eventType) {
      case SUBSCRIPTION:
        messageHandler.onMessage((SubscribedEvent) event);
        break;
      case ORDERBOOK_SNAPSHOT:
        messageHandler.onMessage((OrderbookSnapshot) event);
        break;
      case ORDERBOOK_UPDATE:
        messageHandler.onMessage((OrderbookNotification) event);
        break;
      case TRADES_SNAPSHOT:
      case TRADES_UPDATE:
        messageHandler.onMessage((Trades) event);
        break;
      case ERROR:
        messageHandler.onMessage((ErrorEvent) event);
        break;
    }
  }

  public void subscribeOrderBooks() {
    messageHandler.subscribeAllPairs();
  }


}
