package com.blokaly.ceres.hitbtc.callback;

import com.blokaly.ceres.hitbtc.data.OrderbookSnapshot;
import com.blokaly.ceres.hitbtc.event.EventType;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class OrderBookSnapshotCallbackHandler implements CommandCallbackHandler<OrderbookSnapshot> {
    @Override
    public EventType handleType() {
        return EventType.ORDERBOOK_SNAPSHOT;
    }

    @Override
    public OrderbookSnapshot handleEvent(JsonElement json, JsonDeserializationContext context) {
        JsonObject jsonObject = json.getAsJsonObject();
        JsonObject params = jsonObject.getAsJsonObject("params");
        long sequence = params.get("sequence").getAsLong();
        String symbol = params.get("symbol").getAsString();
        JsonArray asks = params.get("ask").getAsJsonArray();
        JsonArray bids = params.get("bid").getAsJsonArray();
        return OrderbookSnapshot.parse(symbol, sequence, asks, bids);
    }
}
