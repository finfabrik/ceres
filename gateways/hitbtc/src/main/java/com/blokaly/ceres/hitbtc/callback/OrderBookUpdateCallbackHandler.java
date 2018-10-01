package com.blokaly.ceres.hitbtc.callback;

import com.blokaly.ceres.hitbtc.data.OrderbookNotification;
import com.blokaly.ceres.hitbtc.event.EventType;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class OrderBookUpdateCallbackHandler implements CommandCallbackHandler<OrderbookNotification> {
    @Override
    public EventType handleType() {
        return EventType.ORDERBOOK_UPDATE;
    }

    @Override
    public OrderbookNotification handleEvent(JsonElement json, JsonDeserializationContext context) {
        JsonObject jsonObject = json.getAsJsonObject();
        JsonObject params = jsonObject.getAsJsonObject("params");
        long sequence = params.get("sequence").getAsLong();
        String symbol = params.get("symbol").getAsString();
        JsonArray asks = params.get("ask").getAsJsonArray();
        JsonArray bids = params.get("bid").getAsJsonArray();
        return OrderbookNotification.parse(symbol, sequence, asks, bids);

    }
}
