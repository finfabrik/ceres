package com.blokaly.ceres.bitfinex;

import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.network.WSConnectionAdapter;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;

@Singleton
public class BitfinexClientProvider extends WSConnectionAdapter implements Provider<BitfinexClient> {
  private static Logger LOGGER = LoggerFactory.getLogger(BitfinexClientProvider.class);
  private final BitfinexClient client;

  @Inject
  public BitfinexClientProvider(URI serverURI, JsonCracker cracker, @SingleThread ScheduledExecutorService executorService) {
    super(executorService);
    client = new BitfinexClient(serverURI, cracker, this);
  }

  @Override
  public synchronized BitfinexClient get() {
    return client;
  }

  @PreDestroy
  private void stop() {
    diabled = true;
    client.stop();
  }

  @Override
  public void reconnect() {
    if (!diabled) {
      client.stop();
    }
  }

  @Override
  protected void establishConnection() {
    LOGGER.info("Bitfinex client reconnecting...");
    client.reconnect();
  }
}
