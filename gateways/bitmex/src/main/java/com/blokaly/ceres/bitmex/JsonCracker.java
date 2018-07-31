package com.blokaly.ceres.bitmex;

import com.blokaly.ceres.bitmex.event.Close;
import com.blokaly.ceres.bitmex.event.Open;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonCracker {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonCracker.class);
  private final Gson gson;

  private final MessageHandler messageHandler;

  @Inject
  public JsonCracker(Gson gson, MessageHandler messageHandler) {
    this.gson = gson;
    this.messageHandler = messageHandler;
  }

  public void onOpen() {
    messageHandler.onMessage(new Open());
  }

  public void onClose() {
    messageHandler.onMessage(new Close());
  }

  public void crack(String json) {
    LOGGER.debug("event: {}", json);
  }
}
