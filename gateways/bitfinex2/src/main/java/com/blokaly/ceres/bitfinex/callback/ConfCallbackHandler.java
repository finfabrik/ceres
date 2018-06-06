package com.blokaly.ceres.bitfinex.callback;

import com.blokaly.ceres.bitfinex.event.ConfEvent;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;

public class ConfCallbackHandler implements CommandCallbackHandler<ConfEvent> {


  @Override
  public ConfEvent handleEvent(JsonElement json, JsonDeserializationContext context) {
    return context.deserialize(json, ConfEvent.class);
  }
}
