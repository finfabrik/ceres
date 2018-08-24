package com.blokaly.ceres.bitmex;

import com.blokaly.ceres.binding.SingleThread;
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
public class OrderBooks {
  private static final Logger LOGGER = LoggerFactory.getLogger(OrderBooks.class);
  private static final String MEASUREMENT = "OrderBook";
  private static final String NULL_STRING = "-";
  private final Map<String, OrderBasedOrderBook> orderbooks;
  private final BatchedPointsPublisher publisher;
  private final ScheduledExecutorService ses;

  @Inject
  public OrderBooks(Map<String, OrderBasedOrderBook> orderbooks, BatchedPointsPublisher publisher, @SingleThread ScheduledExecutorService ses) {
    this.orderbooks = orderbooks;
    this.publisher = publisher;
    this.ses = ses;
  }

  public ExecutorService getExecutorService() {
    return ses;
  }

  @PostConstruct
  public void start() {
    ses.scheduleWithFixedDelay(this::process, 5, 5, TimeUnit.MINUTES);
  }

  public OrderBasedOrderBook get(String symbol) {
    return orderbooks.get(symbol);
  }

  public Collection<String> getAllSymbols() {
    return orderbooks.keySet();
  }

  public Collection<OrderBasedOrderBook> getAllBooks() {
    return orderbooks.values();
  }

  public void clearAllBooks() {
    orderbooks.values().forEach(OrderBasedOrderBook::clear);
  }

  private void process() {
    orderbooks.values().forEach(this::publishBook);
  }

  public void publishOpen() {
    publisher.publish(builder -> {
      builder.measurement(MEASUREMENT).time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      builder.tag("symbol", NULL_STRING);
      builder.tag("side", NULL_STRING);
      builder.tag("action", "S");
      builder.tag("intraTimestampTag", "0");
      builder.addField("price", 0D);
      builder.addField("quantity", 0D);
    });
  }

  public void publishBook(OrderBasedOrderBook book) {
    publishBook(System.currentTimeMillis(), book);
  }

  public void publishBook(long time, OrderBasedOrderBook book) {
    String symbol = book.getSymbol();
    try {
      if (book.getLastSequence() > 0) {
        Collection<? extends OrderBook.Level> bids = book.getBids();
        Collection<? extends OrderBook.Level> asks = book.getReverseAsks();
        int total = bids.size() + asks.size();
        int length = (int) (Math.log10(total) + 1);
        String intraTimeFormat = "%0" + length + "d";
        final AtomicInteger counter = new AtomicInteger(1);
        bids.forEach(level -> {
          publisher.publish(builder -> {
            buildPoint(time, symbol, "B", "P", intraTimeFormat, counter, level, builder);
          });
        });

        asks.forEach(level -> {
          publisher.publish(builder -> {
            buildPoint(time, symbol, "S", "P", intraTimeFormat, counter, level, builder);
          });
        });
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to process orderbook for " + symbol, ex);
    }
  }

  public void publishDelta(long time, OrderBasedOrderBook book) {
    String symbol = book.getSymbol();
    try {

      Collection<OrderBasedOrderBook.DeltaLevel> delta = book.getDelta();
      int length = (int) (Math.log10(delta.size()) + 1);
      String intraTimeFormat = "%0" + length + "d";
      final AtomicInteger counter = new AtomicInteger(1);
      delta.forEach(level -> {
        publisher.publish(builder -> {
          String side = level.getSide() == OrderInfo.Side.BUY ? "B" : "S";
          buildPoint(time, symbol, side, getAction(level.getType()), intraTimeFormat, counter, level, builder);
        });
      });

    } catch (Exception ex) {
      LOGGER.error("Failed to process delta for " + symbol, ex);
    }
  }

  private void buildPoint(long time, String symbol, String side, String action, String formatter, AtomicInteger counter, OrderBook.Level level, PointBuilderFactory.BatchedPointBuilder builder) {
    builder.measurement(MEASUREMENT).time(time, TimeUnit.MILLISECONDS);
    builder.tag("symbol", symbol);
    builder.tag("side", side);
    builder.tag("action", action);
    builder.tag("intraTimestampTag", String.format(formatter, counter.getAndIncrement()));
    builder.addField("price", level.getPrice().asDbl());
    builder.addField("quantity", level.getQuantity().asDbl());
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
