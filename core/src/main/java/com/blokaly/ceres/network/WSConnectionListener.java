package com.blokaly.ceres.network;

public interface WSConnectionListener {
  void onConnected();
  void onDisconnected();
  void reconnect();
}
