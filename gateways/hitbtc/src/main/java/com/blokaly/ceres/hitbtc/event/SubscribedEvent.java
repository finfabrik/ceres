package com.blokaly.ceres.hitbtc.event;

public class SubscribedEvent extends AbstractEvent{
    private boolean result;
    private long id;

    public boolean getResult() {
        return result;
    }

    public long getSubId() {
        return id;
    }

    SubscribedEvent(){
        super(EventType.SUBSCRIPTION.getType());
    }

    @Override
    public String toString(){
        return "SubscribedEvent{" +
                "method=" + getMethod() +
                ", chanelId=" + id +
                ", result=" + result +
                '}';
    }

}


