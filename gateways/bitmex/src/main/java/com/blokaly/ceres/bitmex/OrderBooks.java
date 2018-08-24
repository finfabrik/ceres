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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Singleton
public class OrderBooks {
  private static final Logger LOGGER = LoggerFactory.getLogger(OrderBooks.class);
  private final Map<String, OrderBasedOrderBook> orderbooks;
  private final BatchedPointsPublisher publisher;
  private final ScheduledExecutorService ses;

  @Inject
  public OrderBooks(Map<String, OrderBasedOrderBook> orderbooks, BatchedPointsPublisher publisher, @SingleThread ScheduledExecutorService ses) {
    this.orderbooks = orderbooks;
    this.publisher = publisher;
    this.ses = ses;
  }

//  @PostConstruct
//  public void start() {
//    ses.scheduleWithFixedDelay(this::process, 10, 10, TimeUnit.SECONDS);
//  }

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

  private void clearBook(OrderBasedOrderBook book) {
    synchronized (book) {
      book.clear();
    }
  }

  private void process() {
    orderbooks.values().forEach(this::publishBook);
  }

  public void publishBook(OrderBasedOrderBook book) {
    String symbol = book.getSymbol();
    try {
      synchronized (book) {
        if (book.getLastSequence() > 0) {
          Collection<? extends OrderBook.Level> bids = book.getBids();
          Collection<? extends OrderBook.Level> asks = book.getReverseAsks();
          int total = bids.size() + asks.size();
          int length = (int)(Math.log10(total)+1);
          String intraTimeFormat = "%0" + length + "d";
          final AtomicInteger counter = new AtomicInteger(1);
          bids.forEach(level -> {
            publisher.publish(builder -> {
              buildPoint(symbol, "B", "P", intraTimeFormat, counter, level, builder);
            });
          });


          asks.forEach(level -> {
            publisher.publish(builder -> {
              buildPoint(symbol, "S", "P", intraTimeFormat, counter, level, builder);
            });
          });
        }
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to process orderbook for " + symbol, ex);
    }
  }

  private void buildPoint(String symbol, String side, String action, String formatter, AtomicInteger counter, OrderBook.Level level, PointBuilderFactory.BatchedPointBuilder builder) {
    builder.measurement("orderbook");
    builder.tag("side", side);
    builder.tag("action", action);
    builder.tag("symbol", symbol);
    builder.tag("intraTimestampTag", String.format(formatter, counter.getAndIncrement()));
    builder.addField("price", level.getPrice().asDbl());
    builder.addField("quantity", level.getQuantity().asDbl());
  }


  public void publishDelta(OrderBasedOrderBook book) {
    String symbol = book.getSymbol();
    try {
      synchronized (book) {
        Collection<OrderBasedOrderBook.DeltaLevel> delta = book.getDelta();
        int length = (int) (Math.log10(delta.size()) + 1);
        String intraTimeFormat = "%0" + length + "d";
        final AtomicInteger counter = new AtomicInteger(1);
        delta.forEach(level -> {
          publisher.publish(builder -> {
            String side = level.getSide() == OrderInfo.Side.BUY ? "B" : "S";
            buildPoint(symbol, side, getAction(level.getType()), intraTimeFormat, counter, level, builder);
          });
        });
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to process delta for " + symbol, ex);
    }
  }

  private String getAction(MarketDataIncremental.Type type) {
    switch (type) {
      case NEW: return "N";
      case UPDATE: return "U";
      case DONE: return "D";
      default: return "-";
    }
  }
}
