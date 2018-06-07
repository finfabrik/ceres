package com.blokaly.ceres.binance;

import com.blokaly.ceres.binance.event.DiffBookEvent;
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
  private final OrderBookHandler handler;
  private final Gson gson;
  private final WSConnectionListener listener;

  public BinanceClient(URI serverURI, OrderBookHandler handler, Gson gson, WSConnectionListener listener) {
    super(serverURI);
    this.handler = handler;
    this.gson = gson;
    this.listener = listener;
    LOGGER.info("client initiated");
  }



  @Override
  public void onOpen(ServerHandshake handshake) {
    LOGGER.info("ws open, status - {}:{}", handshake.getHttpStatus(), handshake.getHttpStatusMessage());
    handler.init();
    if (listener != null) {
      listener.onConnected(handler.getSymbol());
    }
  }

  @Override
  public void onMessage(String message) {
    LOGGER.debug("ws message: {}", message);
    if (!stop) {
      DiffBookEvent diffBookEvent = gson.fromJson(message, DiffBookEvent.class);
      handler.handle(diffBookEvent);
    }
  }

  @Override
  public void onClose(int code, String reason, boolean remote) {
    LOGGER.info("ws close: {}", reason);
    handler.reset();
    if (listener != null) {
      listener.onDisconnected(handler.getSymbol());
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
