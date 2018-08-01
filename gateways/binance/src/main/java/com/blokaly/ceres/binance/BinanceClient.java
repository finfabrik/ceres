package com.blokaly.ceres.binance;

import com.blokaly.ceres.binance.event.DiffBookEvent;
import com.blokaly.ceres.binance.event.StreamEvent;
import com.blokaly.ceres.chronicle.PayloadType;
import com.blokaly.ceres.chronicle.WriteStore;
import com.blokaly.ceres.network.WSConnectionListener;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.inject.Provider;
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
  private final WriteStore store;
  private final Gson gson;
  private final WSConnectionListener listener;
  private final JsonParser parser = new JsonParser();

  public BinanceClient(URI serverURI,
                       OrderBookHandler handler,
                       WriteStore store,
                       Gson gson,
                       WSConnectionListener listener) {
    super(serverURI);
    this.handler = handler;
    this.store = store;
    this.gson = gson;
    this.listener = listener;
    LOGGER.info("client initiated for {}", handler.getSymbol());
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
    store.save(PayloadType.JSON, message);

    StreamEvent event = gson.fromJson(message, StreamEvent.class);
    if (!stop && event.getStream().endsWith(StreamEvent.DEPTH_STREAM)) {
      DiffBookEvent diffBookEvent = gson.fromJson(event.getData(), DiffBookEvent.class);
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
