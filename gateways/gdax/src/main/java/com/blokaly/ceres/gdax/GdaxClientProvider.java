package com.blokaly.ceres.gdax;

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

  public void start() {
    diabled = false;
    client.connect();
  }

  public void stop() {
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
