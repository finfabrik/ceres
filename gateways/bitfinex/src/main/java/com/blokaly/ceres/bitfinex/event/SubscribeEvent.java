package com.blokaly.ceres.bitfinex.event;

public class SubscribeEvent extends AbstractEvent {

    private final String channel;
    private final String prec;
    private final String pair;

    private SubscribeEvent(String pair, String channel, String prec) {
        super("subscribe");
        this.pair = pair;
        this.channel = channel;
        this.prec = prec;
    }

    public static SubscribeEvent orderBookSubscribe(String pair) {
        return new SubscribeEvent(pair, "book", "R0");
    }

    public static SubscribeEvent tradeSubscribe(String pair) {
        return new SubscribeEvent(pair, "trades", null);
    }
}
