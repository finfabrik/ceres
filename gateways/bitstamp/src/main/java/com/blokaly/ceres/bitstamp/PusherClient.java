package com.blokaly.ceres.bitstamp;

import com.blokaly.ceres.bitstamp.event.DiffBookEvent;
import com.blokaly.ceres.bitstamp.event.TradeEvent;
import com.blokaly.ceres.common.PairSymbol;
import com.blokaly.ceres.system.CeresClock;
import com.google.gson.Gson;
import com.pusher.client.Client;
import com.pusher.client.channel.ChannelEventListener;
import com.pusher.client.connection.ConnectionEventListener;
import com.pusher.client.connection.ConnectionState;
import com.pusher.client.connection.ConnectionStateChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PusherClient implements ConnectionEventListener, ChannelEventListener {
  private static final String BTCUSD = "btcusd";
  private static final String TRADE = "trade";
  private static final String DATA = "data";
  private static final String FULL_ORDER_BOOK = "diff_order_book";
  private static final String TICKER = "live_trades";
  private final Logger logger;
  private final PairSymbol pair;
  private final Client pusher;
  private final TradesHandler tradesHandler;
  private final Gson gson;
  private final CeresClock clock;
  private final OrderBookHandler orderBookHandler;

  public PusherClient(Client pusher, CeresClock clock, PairSymbol pair, OrderBookHandler orderBookHandler, TradesHandler tradesHandler, Gson gson) {
    this.pusher = pusher;
    this.clock = clock;
    this.orderBookHandler = orderBookHandler;
    this.tradesHandler = tradesHandler;
    this.gson = gson;
    this.pair = pair;
    logger = LoggerFactory.getLogger(getClass().getName() + "[" + pair.getCode() + "]");
  }

  @Override
  public void onConnectionStateChange(ConnectionStateChange change) {
    logger.info("State changed from {} to {}", change.getPreviousState(), change.getCurrentState());
    if (change.getCurrentState() == ConnectionState.CONNECTED) {
      subscribe();
    }
  }

  @Override
  public void onError(String message, String code, Exception e) {
    logger.error("Pusher connection error: " + message, e);
  }

  private void subscribe() {
    String code = pair.getCode();
    String channelId = BTCUSD.equals(code) ? "" : "_" + code;
    String orderBookChannel = FULL_ORDER_BOOK + channelId;
    pusher.subscribe(orderBookChannel, this, DATA);
    String tradeChannel = TICKER + channelId;
    pusher.subscribe(tradeChannel, this, TRADE);
  }

  @Override
  public void onSubscriptionSucceeded(String channelName) {
    logger.info("{} subscription succeeded", channelName);
    if (channelName != null) {
      switch (channelName) {
        case FULL_ORDER_BOOK:
        {
          orderBookHandler.start();
          break;
        }
        case TICKER:
        {
          tradesHandler.publishOpen(clock.nanos(), pair.toPairString());
          break;
        }
      }
    }
  }

  @Override
  public void onEvent(String channelName, String eventName, String data) {
    logger.debug("{}:{} - {}", channelName, eventName, data);
    long recTime = clock.nanos();
    if (TRADE.equalsIgnoreCase(eventName)) {
      TradeEvent trade = gson.fromJson(data, TradeEvent.class);
      trade.setRecTime(recTime);
      tradesHandler.publishTrades(pair.toPairString(), trade);
    } else {
      DiffBookEvent diffBookEvent = gson.fromJson(data, DiffBookEvent.class);
      orderBookHandler.handle(diffBookEvent);
    }
  }

  public void start() {
    pusher.connect(this, ConnectionState.ALL);
  }

  protected void stop() {
    pusher.disconnect();
  }
}
