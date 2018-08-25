package com.blokaly.ceres.bitmex;

import com.blokaly.ceres.bitmex.event.*;

public interface MessageHandler {
  void onMessage(Open open);
  void onMessage(Close close);
  void onMessage(Subscription subscription);
  void onMessage(Snapshot snapshot);
  void onMessage(Incremental incremental);
  void onMessage(Trades trades);
}
