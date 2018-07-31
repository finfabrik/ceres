package com.blokaly.ceres.bitmex;

import com.blokaly.ceres.bitmex.event.Close;
import com.blokaly.ceres.bitmex.event.Open;
import com.blokaly.ceres.bitmex.event.Snapshot;
import com.blokaly.ceres.bitmex.event.Subscription;
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
  private final Provider<BitmexClient> clientProvider;

  @Inject
  public MessageHandlerImpl(Gson gson, Provider<BitmexClient> clientProvider) {
    this.gson = gson;
    this.clientProvider = clientProvider;
  }

  @Override
  public void onMessage(Open open) {
    LOGGER.info("WS session open");
  }

  @Override
  public void onMessage(Close close) {
    LOGGER.info("WS session close");

  }

  @Override
  public void onMessage(Subscription event) {

  }

  @Override
  public void onMessage(Snapshot event) {

  }
}
