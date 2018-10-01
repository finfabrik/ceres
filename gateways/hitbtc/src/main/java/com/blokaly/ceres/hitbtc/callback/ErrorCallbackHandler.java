package com.blokaly.ceres.hitbtc.callback;

import com.blokaly.ceres.hitbtc.event.ErrorEvent;
import com.blokaly.ceres.hitbtc.event.EventType;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;

public class ErrorCallbackHandler implements CommandCallbackHandler<ErrorEvent> {
    @Override
    public EventType handleType() {
        return EventType.ERROR;
    }

    @Override
    public ErrorEvent handleEvent(JsonElement json, JsonDeserializationContext context) {
        return context.deserialize(json, ErrorEvent.class);
    }
}
