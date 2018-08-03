package com.blokaly.ceres.bitfinex.callback;

import com.blokaly.ceres.bitfinex.event.SubscriptionEvent;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;

public class SubscribedCallbackHandler implements CommandCallbackHandler<SubscriptionEvent> {
    @Override
    public SubscriptionEvent handleEvent(JsonElement json, JsonDeserializationContext context) {
        return context.deserialize(json, SubscriptionEvent.class);
    }
}
