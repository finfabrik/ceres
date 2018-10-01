package com.blokaly.ceres.hitbtc.callback;

import com.blokaly.ceres.hitbtc.event.EventType;
import com.blokaly.ceres.hitbtc.event.SubscribedEvent;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;

public class SubscribedCallbackHandler implements CommandCallbackHandler {

    @Override
    public EventType handleType() {
        return EventType.SUBSCRIPTION;
    }

    @Override
    public SubscribedEvent handleEvent(JsonElement json, JsonDeserializationContext context) {
        return context.deserialize(json, SubscribedEvent.class);
    }
}
