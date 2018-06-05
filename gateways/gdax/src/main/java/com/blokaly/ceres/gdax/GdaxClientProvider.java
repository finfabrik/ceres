package com.blokaly.ceres.gdax;

import com.blokaly.ceres.common.SingleThread;
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
public class GdaxClientProvider extends WSConnectionAdapter implements Provider<GdaxClient> {
  private static Logger LOGGER = LoggerFactory.getLogger(GdaxClientProvider.class);
  private final GdaxClient client;

  @Inject
  public GdaxClientProvider(URI serverURI, JsonCracker cracker, @SingleThread ScheduledExecutorService executorService) {
    super(executorService);
    client = new GdaxClient(serverURI, cracker, this);
  }

  @Override
  public synchronized GdaxClient get() {
    return client;
  }

  @PreDestroy
  private void stop() {
    diabled = true;
    client.stop();
  }

  @Override
  protected void establishConnection() {
    LOGGER.info("Gdax client reconnecting...");
    client.reconnect();
  }

  @Override
  public void reconnect() {
    if (!diabled) {
      client.stop();
    }
  }
}
