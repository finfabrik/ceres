package com.blokaly.ceres.bitfinex;

import com.blokaly.ceres.bitfinex.event.*;

public interface MessageHandler {

    void onMessage(HbEvent event);

    void onMessage(InfoEvent event);

    void onMessage(OrderBookSnapshot event);

    void onMessage(OrderBookRefresh event);

    void onMessage(SubscriptionEvent event);

    void onMessage(PingEvent event);

    void onMessage(PongEvent event);

    void onMessage(ErrorEvent event);
}
