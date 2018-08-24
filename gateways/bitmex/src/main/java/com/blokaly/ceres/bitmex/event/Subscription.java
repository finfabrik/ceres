package com.blokaly.ceres.bitmex.event;

public class Subscription {
  private boolean success;
  private String subscribe;
  private Object request;

  public boolean isSuccess() {
    return success;
  }

  public String getSubscribe() {
    return subscribe;
  }
}
