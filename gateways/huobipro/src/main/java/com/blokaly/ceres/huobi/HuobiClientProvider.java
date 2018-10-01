package com.blokaly.ceres.huobi;

import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.network.WSConnectionAdapter;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;

@Singleton
public class HuobiClientProvider extends WSConnectionAdapter implements Provider<HuobiClient> {
  private static final Logger LOGGER = LoggerFactory.getLogger(HuobiClientProvider.class);
  private final HuobiClient client;

  @Inject
  public HuobiClientProvider(URI serverURI, JsonCracker cracker, @SingleThread ScheduledExecutorService executorService) {
    super(executorService);
    client = new HuobiClient(serverURI, cracker, this);
  }

  @Override
  public synchronized HuobiClient get() {
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
