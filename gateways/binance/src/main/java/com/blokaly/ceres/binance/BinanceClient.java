package com.blokaly.ceres.binance;

import com.blokaly.ceres.binance.event.DiffBookEvent;
import com.blokaly.ceres.binance.event.StreamEvent;
import com.blokaly.ceres.binance.event.TradeEvent;
import com.blokaly.ceres.common.PairSymbol;
import com.blokaly.ceres.network.WSConnectionListener;
import com.google.gson.Gson;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.net.URI;

public class BinanceClient extends WebSocketClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(BinanceClient.class);
  private volatile boolean stop = false;
  private final PairSymbol pair;
  private final OrderBookHandler orderBookHandler;
  private final TradesHandler tradesHandler;
  private final Gson gson;
  private final WSConnectionListener listener;

  public BinanceClient(PairSymbol pair,
                       URI serverURI,
                       OrderBookHandler orderBookHandler,
                       TradesHandler tradesHandler,
                       Gson gson,
                       WSConnectionListener listener) {
    super(serverURI);
    this.pair = pair;
    this.orderBookHandler = orderBookHandler;
    this.tradesHandler = tradesHandler;
    this.gson = gson;
    this.listener = listener;
    LOGGER.info("client initiated for {}", pair.getCode());
  }



  @Override
  public void onOpen(ServerHandshake handshake) {
    LOGGER.info("ws open, status - {}:{}", handshake.getHttpStatus(), handshake.getHttpStatusMessage());
    String symbol = pair.getCode();
    orderBookHandler.init();
    tradesHandler.publishOpen();
    if (listener != null) {
      listener.onConnected(symbol);
    }
  }

  @Override
  public void onMessage(String message) {
    LOGGER.debug("ws message: {}", message);
    if (stop) {
      return;
    }

    StreamEvent event = gson.fromJson(message, StreamEvent.class);
    String channel = event.getChannel();
    switch (channel) {
      case StreamEvent.DEPTH_STREAM: {
        DiffBookEvent diffBookEvent = gson.fromJson(event.getData(), DiffBookEvent.class);
        orderBookHandler.handle(diffBookEvent);
        break;
      }
      case StreamEvent.TRADE_STREAM: {
        TradeEvent trade = gson.fromJson(event.getData(), TradeEvent.class);
        tradesHandler.publishTrades(trade);
        break;
      }
      default: LOGGER.info("Unknown stream channel message: {}", message);
    }
  }

  @Override
  public void onClose(int code, String reason, boolean remote) {
    LOGGER.info("ws close: {}", reason);
    String symbol = orderBookHandler.getSymbol();
    orderBookHandler.reset();
    if (listener != null) {
      listener.onDisconnected(symbol);
    }
  }

  @Override
  public void onError(Exception ex) {
    LOGGER.error("ws error", ex);
  }

  @PreDestroy
  public void stop() {
    stop = true;
    super.close();
  }
}
