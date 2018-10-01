package com.blokaly.ceres.hitbtc.callback;

import com.blokaly.ceres.hitbtc.data.TradesSnapshot;
import com.blokaly.ceres.hitbtc.event.EventType;
import com.blokaly.ceres.system.CeresClock;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.inject.Inject;

public class TradesSnapshotCallbackHandler implements CommandCallbackHandler<TradesSnapshot> {

  private final CeresClock clock;

  @Inject
  public TradesSnapshotCallbackHandler(CeresClock clock) {
    this.clock = clock;
  }

  @Override
  public EventType handleType() {
    return EventType.TRADES_SNAPSHOT;
  }

  @Override
  public TradesSnapshot handleEvent(JsonElement json, JsonDeserializationContext context) {
    JsonObject jsonObject = json.getAsJsonObject();
    JsonObject params = jsonObject.getAsJsonObject("params");
    String symbol = params.get("symbol").getAsString();
    JsonArray data = params.get("data").getAsJsonArray();
    return TradesSnapshot.parse(symbol, data, clock.nanos());
  }
}
