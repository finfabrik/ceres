package com.blokaly.ceres.bitfinex;

import com.blokaly.ceres.bitfinex.event.*;
import com.blokaly.ceres.binding.SingleThread;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

@Singleton
public class JsonCracker {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonCracker.class);
    private final Gson gson;

    private final MessageHandler messageHandler;

    @Inject
    public JsonCracker(Gson gson, MessageHandler messageHandler) {
        this.gson = gson;
        this.messageHandler = messageHandler;
    }

    public void crack(String json) {
        LOGGER.debug("event: {}", json);
        AbstractEvent event = gson.fromJson(json, AbstractEvent.class);

        EventType type = EventType.get(event.getEvent());
        if (type == null) {
            return;
        }
        try {
            switch (type) {
                case HB:
                    messageHandler.onMessage((HbEvent) event);
                    break;
                case PING:
                    messageHandler.onMessage((PingEvent) event);
                    break;
                case PONG:
                    messageHandler.onMessage((PongEvent) event);
                    break;
                case INFO:
                    messageHandler.onMessage((InfoEvent) event);
                    break;
                case SUBSCRIBED:
                    messageHandler.onMessage((SubscriptionEvent) event);
                    break;
                case SNAPSHOT:
                    messageHandler.onMessage((OrderBookSnapshot) event);
                    break;
                case REFRESH:
                    messageHandler.onMessage((OrderBookRefresh) event);
                    break;
                case ERROR:
                    messageHandler.onMessage((ErrorEvent) event);
            }
        } catch (Exception ex) {
            LOGGER.error("Error cracking event", ex);
        }
    }
}
