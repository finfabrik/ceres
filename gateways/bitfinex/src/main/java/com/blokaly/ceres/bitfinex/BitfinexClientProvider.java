package com.blokaly.ceres.bitfinex;

import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.chronicle.WriteStoreProvider;
import com.blokaly.ceres.network.WSConnectionAdapter;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;

@Singleton
public class BitfinexClientProvider extends WSConnectionAdapter implements Provider<BitfinexClient> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BitfinexClientProvider.class);
  private final BitfinexClient client;
  private final WriteStoreProvider storeProvider;

  @Inject
  public BitfinexClientProvider(URI serverURI, WriteStoreProvider storeProvider, JsonCracker cracker, @SingleThread ScheduledExecutorService executorService) {
    super(executorService);
    this.storeProvider = storeProvider;
    client = new BitfinexClient(serverURI, storeProvider.get(), cracker, this);
  }

  @Override
  public synchronized BitfinexClient get() {
    return client;
  }

  public void start() {
    LOGGER.info("Starting...");
    diabled = false;
    storeProvider.begin();
    client.connect();
  }

  public void stop() {
    LOGGER.info("Stopping...");
    diabled = true;
    client.stop();
    storeProvider.end();
  }

  @Override
  public void reconnect(String id) {
    if (!diabled) {
      client.stop();
    }
  }

  @Override
  protected void establishConnection(String id) {
    LOGGER.info("{} reconnecting...", id);
    client.reconnect();
  }
}
