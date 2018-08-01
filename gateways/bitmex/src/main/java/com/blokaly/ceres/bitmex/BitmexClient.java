package com.blokaly.ceres.bitmex;

import com.blokaly.ceres.chronicle.PayloadType;
import com.blokaly.ceres.chronicle.WriteStore;
import com.blokaly.ceres.chronicle.WriteStoreProvider;
import com.blokaly.ceres.network.WSConnectionListener;
import com.google.inject.Inject;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class BitmexClient extends WebSocketClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(BitmexClient.class);
  private static final String client = "bitmex";
  private final WriteStoreProvider storeProvider;
  private final JsonCracker cracker;
  private final WSConnectionListener listener;
  private volatile boolean stop = false;

  @Inject
  public BitmexClient(URI serverURI, WriteStoreProvider storeProvider, JsonCracker cracker, WSConnectionListener listener) {
    super(serverURI);
    this.storeProvider = storeProvider;
    this.cracker = cracker;
    this.listener = listener;
    LOGGER.info("client initiated");
  }


  @Override
  public void onOpen(ServerHandshake handshakedata) {
    LOGGER.info("ws open, status: {}:{}", handshakedata.getHttpStatus(), handshakedata.getHttpStatusMessage());
    storeProvider.begin();
    if (listener != null) {
      listener.onConnected(client);
    }
    cracker.onOpen();
  }

  @Override
  public void onMessage(String message) {
    LOGGER.debug("ws message: {}", message);
    storeProvider.get().save(PayloadType.JSON, message);
    if (!stop) {
      cracker.crack(message);
    }
  }

  @Override
  public void onClose(int code, String reason, boolean remote) {
    LOGGER.info("ws close: {}", reason);
    storeProvider.end();
    if (listener != null) {
      listener.onDisconnected(client);
    }
    cracker.onClose();
  }

  @Override
  public void onError(Exception ex) {
    LOGGER.error("ws error", ex);
  }

  public void stop() {
    stop = true;
    super.close();
  }
}
