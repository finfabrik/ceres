package com.blokaly.ceres.hitbtc.callback;

import com.blokaly.ceres.hitbtc.event.AbstractEvent;
import com.blokaly.ceres.hitbtc.event.EventType;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;

public interface CommandCallbackHandler<T extends AbstractEvent> {
    EventType handleType();
    T handleEvent(JsonElement json, JsonDeserializationContext context);
}
