package com.blokaly.ceres.chronicle;

public interface WriteStore {

  String getPath();

  void save(PayloadType type, String msg);
}
