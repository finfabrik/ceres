package com.blokaly.ceres.bitmex;

import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.chronicle.WriteStoreProvider;
import com.blokaly.ceres.network.WSConnectionAdapter;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;

public class BitmexClientProvider extends WSConnectionAdapter implements Provider<BitmexClient> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BitmexClientProvider.class);
  private final BitmexClient client;

  @Inject
  public BitmexClientProvider(URI serverURI, WriteStoreProvider storeProvider, JsonCracker cracker, @SingleThread ScheduledExecutorService executorService) {
    super(executorService);
    client = new BitmexClient(serverURI, storeProvider, cracker, this);
  }

  @Override
  public synchronized BitmexClient get() {
    return client;
  }

  public void start() {
    LOGGER.info("Starting...");
    diabled = false;
    client.connect();
  }

  public void stop() {
    LOGGER.info("Stopping...");
    diabled = true;
    client.stop();
  }

  @Override
  protected void establishConnection(String id) {
    LOGGER.info("{} reconnecting...", id);
    client.reconnect();
  }

  @Override
  public void reconnect(String id) {
    if (!diabled) {
      client.stop();
    }
  }
}
