package com.blokaly.ceres.chronicle;

import com.blokaly.ceres.binding.CeresModule;
import com.google.inject.Singleton;

public class ChronicleStoreModule  extends CeresModule {

  @Override
  protected void configure() {
    bindExpose(WriteStore.class).toProvider(WriteStoreProvider.class).in(Singleton.class);
  }
}
