package com.blokaly.ceres.binance;

import com.blokaly.ceres.binance.event.StreamEvent;
import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.common.PairSymbol;
import com.blokaly.ceres.common.Source;
import com.blokaly.ceres.influxdb.ringbuffer.BatchedPointsPublisher;
import com.blokaly.ceres.network.WSConnectionAdapter;
import com.blokaly.ceres.orderbook.PriceBasedOrderBook;
import com.blokaly.ceres.orderbook.TopOfBookProcessor;
import com.blokaly.ceres.system.CommonConfigs;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

@Singleton
public class BinanceClientProvider extends WSConnectionAdapter implements Provider<Collection<BinanceClient>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BinanceClientProvider.class);
  private final String wsUrl;
  private final Gson gson;
  private final TopOfBookProcessor processor;
  private final BatchedPointsPublisher publisher;
  private final ScheduledExecutorService ese;
  private final Provider<ExecutorService> esProvider;
  private final Map<String, BinanceClient> clients;
  private final List<String> symbols;
  private final String source;

  @Inject
  public BinanceClientProvider(Config config,
                               Gson gson,
                               TopOfBookProcessor processor,
                               BatchedPointsPublisher publisher,
                               @SingleThread ScheduledExecutorService scheduledExecutorService,
                               @SingleThread Provider<ExecutorService> esProvider
                               ) {
    super(scheduledExecutorService);
    wsUrl = config.getString(CommonConfigs.WS_URL);
    source = Source.valueOf(config.getString(CommonConfigs.APP_SOURCE).toUpperCase()).getCode();
    symbols = config.getStringList("symbols");
    this.gson = gson;
    this.processor = processor;
    this.publisher = publisher;
    this.ese = scheduledExecutorService;
    this.esProvider = esProvider;
    clients = Maps.newHashMap();
  }

  private void init() {
    symbols.forEach(sym -> {
      try {
        PairSymbol pair = PairSymbol.parse(sym);
        String code = pair.getCode();
        URI uri = new URI(wsUrl + getStreams(code));
        OrderBookHandler orderBookHandler = new OrderBookHandler(pair, new PriceBasedOrderBook(code, code + "." + source),
            processor, gson, publisher, ese, esProvider.get());
        TradesHandler tradesHandler = new TradesHandler(pair, publisher);
        BinanceClient client = new BinanceClient(pair, uri, orderBookHandler, tradesHandler, gson, this);
        clients.put(code, client);
      } catch (Exception ex) {
        LOGGER.error("Error creating websocket for symbol: " + sym, ex);
      }
    });
  }

  private String getStreams(String symbol) {
    return symbol + StreamEvent.STREAM_DELIMITER + StreamEvent.DEPTH_STREAM + "/" + symbol + StreamEvent.STREAM_DELIMITER + StreamEvent.TRADE_STREAM;
  }

  @Override
  public Collection<BinanceClient> get() {
    return clients.values();
  }

  public void start() {
    if (disabled) {
      init();
      disabled = false;
      clients.values().forEach(BinanceClient::connect);
    }
  }

  public void stop() {
    if (!disabled) {
      disabled = true;
      clients.values().forEach(BinanceClient::stop);
    }
  }

  @Override
  protected void establishConnection(String id) {
    LOGGER.info("{} reconnecting...", id);
    clients.get(id).reconnect();
  }

  @Override
  public void reconnect(String id) {
    if (!disabled) {
      clients.get(id).stop();
    }
  }
}
