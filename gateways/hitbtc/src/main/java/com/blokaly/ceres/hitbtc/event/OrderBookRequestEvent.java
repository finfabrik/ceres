package com.blokaly.ceres.hitbtc.event;

import java.util.HashMap;
import java.util.Map;

public class OrderBookRequestEvent extends AbstractEvent{

    private final Map<String, String> params;
    private final long id;

    public OrderBookRequestEvent(String pair, long id){
        super("subscribeOrderbook");
        params = new HashMap<>();
        params.put("symbol", pair);
        this.id = id;
    }
}