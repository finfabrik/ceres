package com.blokaly.ceres.hitbtc.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public enum EventType {

    SUBSCRIPTION("subscribe"), ORDERBOOK_SNAPSHOT("snapshotOrderbook"), ORDERBOOK_UPDATE("updateOrderbook"),
    TRADES_SNAPSHOT("snapshotTrades"), TRADES_UPDATE("updateTrades"),
    ERROR("error");

    private static final Map<String, EventType> lookup = new HashMap();
    private static final Logger log = LoggerFactory.getLogger(EventType.class);

    static{
        for(EventType type : EventType.values()) {
            lookup.put(type.getType(), type);
        }
        log.info(lookup.toString());
    }

    private final String type;
    private EventType(String type) { this.type = type; }

    public String getType(){ return type; }

    public static EventType get(String type) {
        if(type==null) return null;
        return lookup.get(type);
    }
}
