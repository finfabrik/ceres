package com.blokaly.ceres.okex;

import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.network.WSConnectionAdapter;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;

public class OKExClientProvider extends WSConnectionAdapter implements Provider<OKExClient> {
  private static final Logger LOGGER = LoggerFactory.getLogger(OKExClientProvider.class);
  private final OKExClient client;

  @Inject
  public OKExClientProvider(URI serverURI, JsonCracker cracker, @SingleThread ScheduledExecutorService executorService) {
    super(executorService);
    client = new OKExClient(serverURI, cracker, this);
  }

  @Override
  public synchronized OKExClient get() {
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
