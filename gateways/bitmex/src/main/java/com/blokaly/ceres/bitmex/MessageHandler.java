package com.blokaly.ceres.bitmex;

import com.blokaly.ceres.bitmex.event.Close;
import com.blokaly.ceres.bitmex.event.Open;
import com.blokaly.ceres.bitmex.event.Snapshot;
import com.blokaly.ceres.bitmex.event.Subscription;

public interface MessageHandler {
  void onMessage(Open open);
  void onMessage(Close close);
  void onMessage(Subscription event);
  void onMessage(Snapshot event);
}
