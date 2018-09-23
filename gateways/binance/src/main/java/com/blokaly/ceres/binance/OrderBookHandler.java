package com.blokaly.ceres.binance;

import com.blokaly.ceres.binance.event.DiffBookEvent;
import com.blokaly.ceres.binance.event.OrderBookEvent;
import com.blokaly.ceres.common.PairSymbol;
import com.blokaly.ceres.data.MarketDataIncremental;
import com.blokaly.ceres.data.OrderInfo;
import com.blokaly.ceres.influxdb.ringbuffer.BatchedPointsPublisher;
import com.blokaly.ceres.influxdb.ringbuffer.PointBuilderFactory;
import com.blokaly.ceres.network.RestGetJson;
import com.blokaly.ceres.orderbook.OrderBook;
import com.blokaly.ceres.orderbook.PriceBasedOrderBook;
import com.blokaly.ceres.orderbook.TopOfBookProcessor;
import com.blokaly.ceres.utils.EventQueueSpliterator;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

public class OrderBookHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(OrderBookHandler.class);
  private static final String ORDER_BOOK_URL = "https://www.binance.com/api/v1/depth?symbol=%s&limit=1000";
  private static final long PAUSE_SECONDS = TimeUnit.SECONDS.toMillis(1);
  private static final String MEASUREMENT = "OrderBook";
  private static final String NULL_STRING = "-";
  private static final String SYMBOL_COL = "symbol";
  private static final String SIDE_COL = "side";
  private static final String ACTION_COL = "action";
  private static final String INTRA_TS_COL = "intraTimestampTag";
  private static final String PRICE_COL = "price";
  private static final String SIZE_COL = "size";
  private final PairSymbol pair;
  private final PriceBasedOrderBook orderBook;
  private final TopOfBookProcessor processor;
  private final Gson gson;
  private final BatchedPointsPublisher publisher;
  private final ScheduledExecutorService ses;
  private final ExecutorService es;
  private final EventQueueSpliterator<DiffBookEvent> splitter;
  private final AtomicBoolean started;
  private final JsonParser parser;

  public OrderBookHandler(PairSymbol pair,
                          PriceBasedOrderBook orderBook,
                          TopOfBookProcessor processor,
                          Gson gson,
                          BatchedPointsPublisher publisher,
                          ScheduledExecutorService scheduledExecutorService,
                          ExecutorService executorService) {
    this.pair = pair;
    this.orderBook = orderBook;
    this.processor = processor;
    this.gson = gson;
    this.publisher = publisher;
    this.ses = scheduledExecutorService;
    this.es = executorService;
    splitter = new EventQueueSpliterator<>();
    started = new AtomicBoolean(false);
    this.parser = new JsonParser();
  }

  public String getSymbol() {
    return orderBook.getSymbol();
  }

  public void init() {

    publishOpen();
    es.execute(orderBook::clear);

    if (started.compareAndSet(false, true)) {
      ses.scheduleWithFixedDelay(()->handle(DiffBookEvent.EMPTY), 5, 5, TimeUnit.MINUTES);

      es.execute(() -> {
        StreamSupport.stream(splitter, false).forEach(event -> {

          if (event == DiffBookEvent.EMPTY) {
            publishBook(orderBook.getLastUpdateTime());
            return;
          }

          long eventTime = event.getEventTime();
          if (orderBook.isInitialized()) {
            if (event.getBeginSequence() <= orderBook.getLastSequence() + 1) {
              orderBook.processIncrementalUpdate(event.getDeletion());
              orderBook.processIncrementalUpdate(event.getUpdate());
              orderBook.setLastUpdateTime(eventTime);
              publishDelta(eventTime);
            }
          } else {
            String symbol = orderBook.getSymbol();
            String url = String.format(ORDER_BOOK_URL, symbol.toUpperCase());
            String jsonResponse = RestGetJson.request(url);
            OrderBookEvent snapshot = gson.fromJson(jsonResponse, OrderBookEvent.class);
            LOGGER.debug("{}", snapshot);
            while (snapshot.getSequence() <= event.getEndSequence()) {
              try {
                Thread.sleep(PAUSE_SECONDS);
                jsonResponse = RestGetJson.request(url);
                snapshot = gson.fromJson(jsonResponse, OrderBookEvent.class);
                LOGGER.debug("{}", snapshot);
              } catch (InterruptedException e) {
                if (Thread.currentThread().isInterrupted()) {
                  LOGGER.info("Retrieving snapshot interrupted, quitting...");
                  break;
                }
              }
            }

            JsonObject jsonObj = new JsonObject();
            jsonObj.addProperty("stream", symbol + "@snapshot");
            jsonObj.add("data", parser.parse(jsonResponse));
            orderBook.processSnapshot(snapshot);
            publishBook(eventTime);
          }
          processor.process(orderBook);
        });
      });
    }
  }

  public void reset() {
    es.execute(()->{
      orderBook.clear();
      processor.process(orderBook);
    });
  }

  public void handle(DiffBookEvent event) {
    splitter.add(event);
  }

  public void publishOpen() {
    publisher.publish(builder -> {
      buildPoint(System.currentTimeMillis(), pair.toPairString(), NULL_STRING, "S", "0", 0D, 0D, builder);
    });
  }

  private void publishBook(long time) {
    if (orderBook.getLastSequence() <= 0) {
      return;
    }

    String symbol = pair.toPairString();
    LOGGER.info("Storing orderbook snapshot for {}", symbol);

    try {
      Collection<? extends OrderBook.Level> bids = orderBook.getBids();
      Collection<? extends OrderBook.Level> asks = orderBook.getReverseAsks();
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

  private void publishDelta(long time) {
    String symbol = pair.toPairString();
    try {
      Collection<PriceBasedOrderBook.DeltaLevel> delta = orderBook.getDelta();
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
