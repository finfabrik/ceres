package com.blokaly.ceres.hitbtc.event;

import com.blokaly.ceres.hitbtc.data.OrderbookNotification;

public class UpdateEvent extends AbstractEvent {

    private OrderbookNotification notification;

    UpdateEvent(String method, OrderbookNotification notification){
        super(method);
        this.notification = notification;
    }

    public void setNotification(final OrderbookNotification notification){
        this.notification = notification;
    }

    public OrderbookNotification getNotification(){
        return notification;
    }

    @Override
    public String toString(){
        return "UpdateEvent{" +
                "method=" + getMethod() +
                ", sequence=" + notification.getSequence() +
                ", symbol=" + notification.getSymbol() +
                ", updates=" + notification.getUpdate() +
                ", bids=" + notification.getDeletion() +
                "}";
    }


}
