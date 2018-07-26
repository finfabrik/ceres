package com.blokaly.ceres.orderbook;

public interface TopOfBookProcessor {

  class NoOpProcessor implements TopOfBookProcessor {
    @Override
    public void process(TopOfBook topOfBook) {}
  }

  void process(TopOfBook topOfBook);
}
