package com.blokaly.ceres.bitfinex;

import com.blokaly.ceres.binding.SingleThread;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Singleton
public class BitfinexClientProvider implements Provider<BitfinexClient>, BitfinexClient.ConnectionListener {
  private static Logger LOGGER = LoggerFactory.getLogger(BitfinexClientProvider.class);
  private final BitfinexClient client;
  private final ScheduledExecutorService executorService;
  private volatile boolean stopping;

  @Inject
  public BitfinexClientProvider(URI serverURI, JsonCracker cracker, @SingleThread ScheduledExecutorService executorService) {
    this.executorService = executorService;
    client = new BitfinexClient(serverURI, cracker, this);
    stopping = false;
  }

  @Override
  public synchronized BitfinexClient get() {
    return client;
  }

  @Override
  public void onConnected() {
    LOGGER.info("Bitfinex client connected");
  }

  @Override
  public void onDisconnected() {
    LOGGER.info("Bitfinex client disconnected");
    if (!stopping) {
      executorService.schedule(this::connectWorker, 5, TimeUnit.SECONDS);
    }
  }

  @PreDestroy
  private void stop() {
    stopping = true;
    client.stop();
  }

  @Override
  public void reconnect() {
    if (!stopping) {
      client.stop();
    }
  }

  private void connectWorker() {
    if (!stopping) {
      LOGGER.info("Bitfinex client reconnecting...");
      client.reconnect();
    }
  }
}
