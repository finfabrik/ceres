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
  private final WriteStoreProvider storeProvider;

  @Inject
  public BitmexClientProvider(URI serverURI, WriteStoreProvider storeProvider, JsonCracker cracker, @SingleThread ScheduledExecutorService executorService) {
    super(executorService);
    this.storeProvider = storeProvider;
    client = new BitmexClient(serverURI, storeProvider.get(), cracker, this);
  }

  @Override
  public synchronized BitmexClient get() {
    return client;
  }

  public void start() {
    diabled = false;
    storeProvider.begin();
    client.connect();
  }

  public void stop() {
    diabled = true;
    client.stop();
    storeProvider.end();
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
