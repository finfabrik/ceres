package com.blokaly.ceres.hitbtc;

import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.common.PairSymbol;
import com.blokaly.ceres.common.Source;
import com.blokaly.ceres.data.MarketDataIncremental;
import com.blokaly.ceres.data.OrderInfo;
import com.blokaly.ceres.hitbtc.data.OrderbookNotification;
import com.blokaly.ceres.hitbtc.data.OrderbookSnapshot;
import com.blokaly.ceres.hitbtc.event.OrderBookRequestEvent;
import com.blokaly.ceres.influxdb.ringbuffer.BatchedPointsPublisher;
import com.blokaly.ceres.influxdb.ringbuffer.PointBuilderFactory;
import com.blokaly.ceres.orderbook.OrderBasedOrderBook;
import com.blokaly.ceres.orderbook.OrderBook;
import com.blokaly.ceres.orderbook.PriceBasedOrderBook;
import com.blokaly.ceres.orderbook.TopOfBookProcessor;
import com.blokaly.ceres.system.CommonConfigs;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Singleton
public class OrderBookHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(OrderBookHandler.class);
  private static final String MEASUREMENT = "OrderBook";
  private static final String NULL_STRING = "-";
  private static final String SYMBOL_COL = "symbol";
  private static final String SIDE_COL = "side";
  private static final String ACTION_COL = "action";
  private static final String INTRA_TS_COL = "intraTimestampTag";
  private static final String PRICE_COL = "price";
  private static final String SIZE_COL = "size";
  private final Map<String, PriceBasedOrderBook> orderbooks;
  private final Map<Long, String> subMap;
  private final Map<String, PairSymbol> symbols;
  private final TopOfBookProcessor tobProcessor;
  private final BatchedPointsPublisher publisher;
  private final ScheduledExecutorService ses;

  @Inject
  public OrderBookHandler(Config config,
                          @Named("SymbolMap") Map<String, PairSymbol> symbols,
                          TopOfBookProcessor tobProcessor,
                          BatchedPointsPublisher publisher,
                          @SingleThread ScheduledExecutorService ses) {
    String source = Source.valueOf(config.getString(CommonConfigs.APP_SOURCE).toUpperCase()).getCode();
    this.symbols = symbols;
    orderbooks = symbols.values().stream()
        .collect(Collectors.toMap(pair -> pair.getCode().toUpperCase(), pair -> {
          String key = pair.getCode() + "." + source;
          return new PriceBasedOrderBook(pair.getCode(), key);
        }));
    subMap = Maps.newConcurrentMap();
    this.tobProcessor = tobProcessor;
    this.publisher = publisher;
    this.ses = ses;
  }

  @PostConstruct
  public void start() {
    ses.scheduleWithFixedDelay(this::processOrderBooks, 5, 5, TimeUnit.MINUTES);
  }

  public PriceBasedOrderBook get(String symbol) {
    return orderbooks.get(symbol);
  }

  public Collection<String> getAllChannels() {
    return orderbooks.keySet();
  }

  public Collection<PriceBasedOrderBook> getAllBooks() {
    return orderbooks.values();
  }

  public void resetOrderBook(long subId) {
    if (subMap.containsKey(subId)) {
      orderbooks.get(subMap.get(subId)).clear();
    }
  }

  public void publishOpen() {
    long now = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
    symbols.values().forEach(symbol -> {
      publisher.publish(builder -> {
        buildPoint(now, symbol.toPairString(), NULL_STRING, "S", "0", 0D, 0D, builder);
      });
    });
  }

  public void subscribeAll(HitbtcClient sender, Gson gson) {
    subMap.clear();
    orderbooks.keySet().forEach(sym -> {
      long id = System.nanoTime();
      subMap.put(id, sym);
      try {
        String jsonString = gson.toJson(new OrderBookRequestEvent(sym, id));
        LOGGER.info("Subscribing to order book[{}]...", sym);
        sender.send(jsonString);
      } catch (Exception e) {
        LOGGER.error("Error subscribing to order book " + sym, e);
      }
    });
  }

  public void processSnapshot(OrderbookSnapshot snapshot) {
    String symbol = snapshot.getSymbol();
    PriceBasedOrderBook orderBook = orderbooks.get(symbol);
    if (orderBook == null) {
      return;
    }

    ses.execute(() -> {
      orderBook.processSnapshot(snapshot);
      orderBook.setLastUpdateTime(snapshot.getTime());
      publishBook(orderBook);
    });

    tobProcessor.process(orderBook);
  }

  public void processIncremental(OrderbookNotification update) {
    PriceBasedOrderBook book = orderbooks.get(update.getSymbol());
    if (book == null) {
      return;
    }

    ses.execute(() -> {
      book.setLastUpdateTime(update.getTime());
      book.processIncrementalUpdate(update.getDeletion());
      book.processIncrementalUpdate(update.getUpdate());
      publishDelta(book);
      book.clearDelta();
    });

    tobProcessor.process(book);
  }

  private void processOrderBooks() {
    orderbooks.values().forEach(this::publishBook);
  }

  private void publishBook(PriceBasedOrderBook book) {
    if (book.getLastSequence() <= 0) {
      return;
    }

    String symbol = symbols.get(book.getSymbol()).toPairString();
    LOGGER.info("Storing orderbook snapshot for {}", symbol);

    try {
      long time = book.getLastUpdateTime();
      Collection<? extends OrderBook.Level> bids = book.getBids();
      Collection<? extends OrderBook.Level> asks = book.getReverseAsks();
      int total = bids.size() + asks.size();
      int length = (int) (Math.log10(total) + 1);
      String intraTimeFormat = "%0" + length + "d";
      final AtomicInteger counter = new AtomicInteger(1);
      bids.forEach(level -> {
        publisher.publish(builder -> {
          String intraTs = String.format(intraTimeFormat, counter.getAndIncrement());
          double price = level.getPrice().asDbl();
          double size = level.getQuantity().asDbl();
          buildPoint(time, symbol, "B", "P", intraTs, price, size, builder);
        });
      });

      asks.forEach(level -> {
        publisher.publish(builder -> {
          String intraTs = String.format(intraTimeFormat, counter.getAndIncrement());
          double price = level.getPrice().asDbl();
          double size = level.getQuantity().asDbl();
          buildPoint(time, symbol, "S", "P", intraTs, price, size, builder);
        });
      });

    } catch (Exception ex) {
      LOGGER.error("Failed to process orderbook for " + symbol, ex);
    }
  }

  private void publishDelta(PriceBasedOrderBook book) {
    String symbol = symbols.get(book.getSymbol()).toPairString();
    try {
      long time = book.getLastUpdateTime();
      Collection<PriceBasedOrderBook.DeltaLevel> delta = book.getDelta();
      int length = (int) (Math.log10(delta.size()) + 1);
      String intraTimeFormat = "%0" + length + "d";
      final AtomicInteger counter = new AtomicInteger(1);
      delta.forEach(level -> {
        publisher.publish(builder -> {
          String side = level.getSide() == OrderInfo.Side.BUY ? "B" : "S";
          String intraTs = String.format(intraTimeFormat, counter.getAndIncrement());
          double price = level.getPrice().asDbl();
          double size = level.getQuantity().asDbl();
          buildPoint(time, symbol, side, getAction(level.getType()), intraTs, price, size, builder);
        });
      });

    } catch (Exception ex) {
      LOGGER.error("Failed to process delta for " + symbol, ex);
    }
  }

  private String getAction(MarketDataIncremental.Type type) {
    switch (type) {
      case NEW:
        return "N";
      case UPDATE:
        return "U";
      case DONE:
        return "D";
      default:
        return NULL_STRING;
    }
  }

  private void buildPoint(long time, String symbol, String side, String action, String intraTs, double price, double size, PointBuilderFactory.BatchedPointBuilder builder) {
    builder.measurement(MEASUREMENT).time(time, TimeUnit.NANOSECONDS);
    builder.tag(SYMBOL_COL, symbol.toUpperCase());
    builder.tag(SIDE_COL, side);
    builder.tag(ACTION_COL, action);
    builder.tag(INTRA_TS_COL, intraTs);
    builder.addField(PRICE_COL, price);
    builder.addField(SIZE_COL, size);
  }
}