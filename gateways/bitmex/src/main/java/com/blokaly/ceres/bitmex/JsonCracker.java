package com.blokaly.ceres.bitmex;

import com.blokaly.ceres.bitmex.event.*;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
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
    JsonObject jsonObject = gson.fromJson(json, JsonElement.class).getAsJsonObject();
    try {
      if (jsonObject.has("table")) {
        String table = jsonObject.get("table").getAsString();
        if ("orderBookL2".equalsIgnoreCase(table)) {
          if ("partial".equalsIgnoreCase(jsonObject.get("action").getAsString())) {
            Snapshot snapshot = gson.fromJson(json, Snapshot.class);
            messageHandler.onMessage(snapshot);
          } else {
            Incremental incremental = gson.fromJson(json, Incremental.class);
            messageHandler.onMessage(incremental);
          }
        } else if ("trade".equalsIgnoreCase(table)) {
          Trades trades = gson.fromJson(json, Trades.class);
          messageHandler.onMessage(trades);
        }
      } else if (jsonObject.has("subscribe")) {
        Subscription subscription = gson.fromJson(jsonObject, Subscription.class);
        messageHandler.onMessage(subscription);
      }
    } catch (Exception ex) {
      LOGGER.error("Error cracking event: " + json, ex);
    }
  }
}
