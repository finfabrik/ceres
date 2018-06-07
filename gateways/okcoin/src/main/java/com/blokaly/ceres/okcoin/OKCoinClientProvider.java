package com.blokaly.ceres.okcoin;

import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.network.WSConnectionAdapter;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;

public class OKCoinClientProvider  extends WSConnectionAdapter implements Provider<OKCoinClient> {
  private static final Logger LOGGER = LoggerFactory.getLogger(OKCoinClientProvider.class);
  private final OKCoinClient client;

  @Inject
  public OKCoinClientProvider(URI serverURI, JsonCracker cracker, @SingleThread ScheduledExecutorService executorService) {
    super(executorService);
    client = new OKCoinClient(serverURI, cracker, this);
  }

  @Override
  public synchronized OKCoinClient get() {
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
