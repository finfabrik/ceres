package com.blokaly.ceres.cex;

import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.network.WSConnectionAdapter;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;

public class CexClientProvider extends WSConnectionAdapter implements Provider<CexClient> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CexClientProvider.class);
  private final CexClient client;

  @Inject
  public CexClientProvider(Config config, URI serverURI, JsonCracker cracker, @SingleThread ScheduledExecutorService executorService) {
    super(executorService);
    client = new CexClient(config, serverURI, cracker, this);
  }

  @Override
  public synchronized CexClient get() {
    return client;
  }

  public void start() {
    disabled = false;
    client.connect();
  }

  public void stop() {
    disabled = true;
    client.stop();
  }

  @Override
  protected void establishConnection(String id) {
    LOGGER.info("{} reconnecting...", id);
    client.reconnect();
  }

  @Override
  public void reconnect(String id) {
    if (!disabled) {
      client.stop();
    }
  }
}
