package com.blokaly.ceres.network;

public interface WSConnectionListener {
  void onConnected(String id);
  void onDisconnected(String id);
  void reconnect(String id);
}
