package com.blokaly.ceres.hitbtc.event;

import com.blokaly.ceres.hitbtc.data.OrderbookSnapshot;

public abstract class ChannelEvent extends AbstractEvent{
    protected OrderbookSnapshot orderBookSnapshot;

    ChannelEvent(String method, OrderbookSnapshot orderBookSnapshot){
        super(method);
        this.orderBookSnapshot = orderBookSnapshot;
    }

    public OrderbookSnapshot getOrderBookSnapshot() {
        return orderBookSnapshot;
    }

    public void setOrderBookSnapshot(OrderbookSnapshot orderBookSnapshot) {
        this.orderBookSnapshot = orderBookSnapshot;
    }
}
