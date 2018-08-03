package com.blokaly.ceres.bitfinex;

import com.blokaly.ceres.bitfinex.event.*;
import com.blokaly.ceres.orderbook.OrderBasedOrderBook;
import com.blokaly.ceres.orderbook.TopOfBookProcessor;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PublicKey;
import java.util.concurrent.ConcurrentMap;

@Singleton
public class MessageHandlerImpl implements MessageHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageHandlerImpl.class);
  private final Gson gson;
  private final Provider<BitfinexClient> clientProvider;
  private final ConcurrentMap<Integer, SubscriptionEvent> channelMap;
  private final OrderBookKeeper bookKeeper;
  private final TopOfBookProcessor processor;

  @Inject
  public MessageHandlerImpl(Gson gson,
                            Provider<BitfinexClient> clientProvider,
                            @Named("ChannelMap") ConcurrentMap<Integer, SubscriptionEvent> channelMap,
                            OrderBookKeeper bookKeeper,
                            TopOfBookProcessor processor) {
    this.gson = gson;
    this.clientProvider = clientProvider;
    this.channelMap = channelMap;
    this.bookKeeper = bookKeeper;
    this.processor = processor;
  }

  @Override
  public void onMessage(HbEvent event) {
    LOGGER.debug("HB[{}]", bookKeeper.getSymbol(event.getChannelId()));
  }

  @Override
  public void onMessage(PingEvent event) {
    LOGGER.info("PING");
  }

  @Override
  public void onMessage(PongEvent event) {
    LOGGER.info("PONG");
  }

  @Override
  public void onMessage(ErrorEvent event) {
    LOGGER.error("{}", event);
  }

  @Override
  public void onMessage(InfoEvent event) {
    LOGGER.info("Received info {}", event);
    String version = event.getVersion();
    if (version != null) {
      String[] vers = version.split("\\.");
      if (!"1".equals(vers[0])) {
        LOGGER.error("Unsupported version: {}, only v1 supported.", version);
        return;
      }
      bookKeeper.getAllSymbols().forEach(this::subscribe);
    } else {
      InfoEvent.Status status = event.getStatus();
      if (status == InfoEvent.Status.WEB_SOCKET_RESTART || status == InfoEvent.Status.PAUSE) {
        LOGGER.warn("Status is {}, resetting orderbooks...", status);
        bookKeeper.getAllBooks().forEach(book -> {
          book.clear();
          processor.process(book);
        });
        channelMap.clear();
      } else if (status == InfoEvent.Status.RESUME) {
        LOGGER.warn("Status is {}, reconnect ws", status);
        clientProvider.get().tryReconnect();
      }
    }

  }

  private void subscribe(String symbol) {
    BitfinexClient sender = clientProvider.get();
    if (sender == null) {
      LOGGER.error("Bitfinex client unavailable, skip subscription for " + symbol);
      return;
    }
    String jsonString = gson.toJson(SubscribeEvent.orderBookSubscribe(symbol));
    LOGGER.info("subscribing: {}", jsonString);
    sender.send(jsonString);

    jsonString = gson.toJson(SubscribeEvent.tradeSubscribe(symbol));
    LOGGER.info("subscribing: {}", jsonString);
    sender.send(jsonString);
  }

  @Override
  public void onMessage(SubscriptionEvent event) {
    int chanId = event.getChanId();
    channelMap.put(chanId, event);
    if (ChannelEvent.ORDERBOOK_CHANNEL.equalsIgnoreCase(event.getChannel())) {
      bookKeeper.makeOrderBook(chanId, event.getPair());
    }
  }

  @Override
  public void onMessage(OrderBookSnapshot event) {
    OrderBasedOrderBook orderBook = bookKeeper.get(event.getChannelId());
    if (orderBook == null) {
      throw new IllegalStateException("No order book for channel id " + event.getChannelId());
    }
    orderBook.processSnapshot(event);
  }

  @Override
  public void onMessage(OrderBookRefresh event) {
    OrderBasedOrderBook orderBook = bookKeeper.get(event.getChannelId());
    if (orderBook == null) {
      throw new IllegalStateException("No order book for channel id " + event.getChannelId());
    }
    orderBook.processIncrementalUpdate(event);
    processor.process(orderBook);
  }
}
