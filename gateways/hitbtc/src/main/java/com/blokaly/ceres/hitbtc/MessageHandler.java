package com.blokaly.ceres.hitbtc;

import com.blokaly.ceres.hitbtc.data.OrderbookNotification;
import com.blokaly.ceres.hitbtc.data.OrderbookSnapshot;
import com.blokaly.ceres.hitbtc.data.Trades;
import com.blokaly.ceres.hitbtc.event.ErrorEvent;
import com.blokaly.ceres.hitbtc.event.SubscribedEvent;

public interface MessageHandler {

  void subscribeAllPairs();

  void onMessage(SubscribedEvent event);

  void onMessage(OrderbookSnapshot event);

  void onMessage(OrderbookNotification event);

  void onMessage(ErrorEvent event);

  void onMessage(Trades event);
}
