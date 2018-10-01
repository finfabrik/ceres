package com.blokaly.ceres.hitbtc;

import com.blokaly.ceres.hitbtc.callback.CommandCallbackHandler;
import com.blokaly.ceres.hitbtc.event.AbstractEvent;
import com.blokaly.ceres.hitbtc.event.EventType;
import com.blokaly.ceres.hitbtc.event.NoOpEvent;
import com.google.gson.*;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Map;

import static com.blokaly.ceres.hitbtc.event.EventType.ERROR;
import static com.blokaly.ceres.hitbtc.event.EventType.SUBSCRIPTION;

public class EventAdapter implements JsonDeserializer<AbstractEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventAdapter.class);
    private final Map<EventType, CommandCallbackHandler> handlers;

    private final NoOpEvent noOpEvent = new NoOpEvent();

    @Inject
    EventAdapter(Map<EventType, CommandCallbackHandler> handlers){
        this.handlers = handlers;
    }

    @Override
    public AbstractEvent deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        EventType eventType = null;
        if(json.isJsonObject()){
            JsonObject jsonObject = json.getAsJsonObject();
            if(jsonObject.has("method")) eventType = EventType.get(jsonObject.get("method").getAsString());
            if(jsonObject.has("error")) return handlers.get(ERROR).handleEvent(json, context);
            else if(jsonObject.has("result")) return handlers.get(SUBSCRIPTION).handleEvent(json, context);
            else if((eventType != null) && (handlers.get(eventType) != null)) return handlers.get(eventType).handleEvent(json, context);
        }
        return noOpEvent;
    }
}
