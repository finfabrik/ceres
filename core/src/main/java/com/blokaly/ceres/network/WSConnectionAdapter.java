package com.blokaly.ceres.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class WSConnectionAdapter implements WSConnectionListener {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final ScheduledExecutorService executorService;
  protected volatile boolean diabled;

  protected WSConnectionAdapter(ScheduledExecutorService executorService) {
    this.executorService = executorService;
  }

  @Override
  public void onConnected() {
    logger.info("WS client connected");
  }

  @Override
  public void onDisconnected() {
    logger.info("WS client disconnected");
    if (!diabled) {
      executorService.schedule(this::establishConnection, 5, TimeUnit.SECONDS);
    }
  }

  abstract protected void establishConnection();
}
