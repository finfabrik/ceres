package com.blokaly.ceres.binance;

import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.chronicle.WriteStore;
import com.blokaly.ceres.common.Source;
import com.blokaly.ceres.data.SymbolFormatter;
import com.blokaly.ceres.network.WSConnectionAdapter;
import com.blokaly.ceres.orderbook.PriceBasedOrderBook;
import com.blokaly.ceres.orderbook.TopOfBookProcessor;
import com.blokaly.ceres.system.CommonConfigs;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class BinanceClientProvider extends WSConnectionAdapter implements Provider<Collection<BinanceClient>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BinanceClientProvider.class);
  private final String wsUrl;
  private final Gson gson;
  private final TopOfBookProcessor processor;
  private final WriteStore store;
  private final Provider<ExecutorService> esProvider;
  private final Map<String, BinanceClient> clients;
  private final List<String> symbols;
  private final String source;

  @Inject
  public BinanceClientProvider(Config config,
                               Gson gson,
                               TopOfBookProcessor processor,
                               WriteStore store,
                               @SingleThread Provider<ExecutorService> esProvider,
                               @SingleThread ScheduledExecutorService executorService
                               ) {
    super(executorService);
    wsUrl = config.getString(CommonConfigs.WS_URL);
    source = Source.valueOf(config.getString(CommonConfigs.APP_SOURCE).toUpperCase()).getCode();
    symbols = config.getStringList("symbols");
    this.gson = gson;
    this.processor = processor;
    this.store = store;
    this.esProvider = esProvider;
    clients = Maps.newHashMap();
  }

  @PostConstruct
  private void init() {
    symbols.forEach(sym -> {
      try {
        String symbol = SymbolFormatter.normalise(sym);
        URI uri = new URI(String.format(wsUrl, sym));
        OrderBookHandler handler = new OrderBookHandler(new PriceBasedOrderBook(symbol, symbol + "." + source), processor, gson, store, esProvider.get());
        BinanceClient client = new BinanceClient(uri, handler, store, gson, this);
        clients.put(symbol, client);
      } catch (Exception ex) {
        LOGGER.error("Error creating websocket for symbol: " + sym, ex);
      }
    });
  }

  @Override
  public Collection<BinanceClient> get() {
    return clients.values();
  }

  public void start() {
    diabled = false;
    clients.values().forEach(BinanceClient::connect);
  }

  public void stop() {
    diabled = true;
    clients.values().forEach(BinanceClient::stop);
  }

  @Override
  protected void establishConnection(String id) {
    LOGGER.info("{} reconnecting...", id);
    clients.get(id).reconnect();
  }

  @Override
  public void reconnect(String id) {
    if (!diabled) {
      clients.get(id).stop();
    }
  }
}
