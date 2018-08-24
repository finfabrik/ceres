package com.blokaly.ceres.bitmex;

import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.bitmex.event.*;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

public class JsonCracker {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonCracker.class);
  private final Gson gson;

  private final MessageHandler messageHandler;
  private final ExecutorService executor;

  @Inject
  public JsonCracker(Gson gson, MessageHandler messageHandler, @SingleThread ExecutorService executor) {
    this.gson = gson;
    this.messageHandler = messageHandler;
    this.executor = executor;
  }

  public void onOpen() {
    messageHandler.onMessage(new Open());
  }

  public void onClose() {
    messageHandler.onMessage(new Close());
  }

  public void crack(String json) {
    LOGGER.info("event: {}", json);
    executor.execute(() -> process(json));
  }

  private void process(String json) {
    JsonObject jsonObject = gson.fromJson(json, JsonElement.class).getAsJsonObject();
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
      }
    } else if (jsonObject.has("subscribe")) {
      Subscription subscription = gson.fromJson(jsonObject, Subscription.class);
      messageHandler.onMessage(subscription);
    }
  }
}
