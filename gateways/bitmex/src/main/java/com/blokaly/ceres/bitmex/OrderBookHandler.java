package com.blokaly.ceres.bitmex;

import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.bitmex.event.Incremental;
import com.blokaly.ceres.bitmex.event.Snapshot;
import com.blokaly.ceres.data.MarketDataIncremental;
import com.blokaly.ceres.data.OrderInfo;
import com.blokaly.ceres.influxdb.ringbuffer.BatchedPointsPublisher;
import com.blokaly.ceres.influxdb.ringbuffer.PointBuilderFactory;
import com.blokaly.ceres.orderbook.OrderBasedOrderBook;
import com.blokaly.ceres.orderbook.OrderBook;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
  private final Map<String, OrderBasedOrderBook> orderbooks;
  private final BatchedPointsPublisher publisher;
  private final ScheduledExecutorService ses;

  @Inject
  public OrderBookHandler(Map<String, OrderBasedOrderBook> orderbooks, BatchedPointsPublisher publisher, @SingleThread ScheduledExecutorService ses) {
    this.orderbooks = orderbooks;
    this.publisher = publisher;
    this.ses = ses;
  }

  @PostConstruct
  public void start() {
    ses.scheduleWithFixedDelay(this::processOrderBooks, 5, 5, TimeUnit.MINUTES);
  }

  public Collection<String> getAllSymbols() {
    return orderbooks.keySet();
  }

  public void clearAllBooks() {
    ses.execute(()->{
      orderbooks.values().forEach(OrderBasedOrderBook::clear);
    });

  }

  private void processOrderBooks() {
    orderbooks.values().forEach(this::publishBook);
  }

  public void publishOpen() {
    getAllSymbols().forEach(symbol -> {
      publisher.publish(builder -> {
        buildPoint(System.currentTimeMillis(), symbol, NULL_STRING, "S", "0", 0D, 0D, builder);
      });
    });
  }

  public void processSnapshot(Snapshot snapshot) {

    OrderBasedOrderBook book = orderbooks.get(snapshot.getSymbol());
    if (book == null) {
      return;
    }

    ses.execute(()->{
      book.processSnapshot(snapshot);
      publishBook(snapshot.getTime(), book);
    });
  }

  public void processIncremental(Incremental incremental) {
    OrderBasedOrderBook book = orderbooks.get(incremental.getSymbol());
    if (book == null) {
      return;
    }

    ses.execute(()->{
      book.processIncrementalUpdate(incremental);
      publishDelta(incremental.getTime(), book);
    });
  }

  private void publishBook(OrderBasedOrderBook book) {
    publishBook(System.currentTimeMillis(), book);
  }

  private void publishBook(long time, OrderBasedOrderBook book) {
    if (book.getLastSequence() <= 0) {
      return;
    }

    String symbol = book.getSymbol();
    try {
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

  private void publishDelta(long time, OrderBasedOrderBook book) {
    String symbol = book.getSymbol();
    try {

      Collection<OrderBasedOrderBook.DeltaLevel> delta = book.getDelta();
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

  private void buildPoint(long time, String symbol, String side, String action, String intraTs, double price, double size, PointBuilderFactory.BatchedPointBuilder builder) {
    builder.measurement(MEASUREMENT).time(time, TimeUnit.MILLISECONDS);
    builder.tag(SYMBOL_COL, symbol.toUpperCase());
    builder.tag(SIDE_COL, side);
    builder.tag(ACTION_COL, action);
    builder.tag(INTRA_TS_COL, intraTs);
    builder.addField(PRICE_COL, price);
    builder.addField(SIZE_COL, size);
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
}
