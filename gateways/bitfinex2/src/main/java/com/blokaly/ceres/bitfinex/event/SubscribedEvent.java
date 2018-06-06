package com.blokaly.ceres.bitfinex.event;

public class SubscribedEvent extends AbstractEvent {
    private String channel;
    private int chanId;
    private String symbol;
    private String prec;
    private String freq;
    private String len;
    private String pair;

    public int getChanId() {
        return chanId;
    }

    public String getPair() {
        return pair;
    }

    @Override
    public String toString() {
        return "SubscribedEvent{" +
                "channel=" + channel +
                ", chanId=" + chanId +
                ", symbol=" + symbol +
                ", prec=" + prec +
                ", freq=" + freq +
                ", len=" + len +
                ", pair=" + pair +
                '}';
    }
}
