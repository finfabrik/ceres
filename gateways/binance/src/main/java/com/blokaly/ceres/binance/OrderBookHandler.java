package com.blokaly.ceres.binance;

import com.blokaly.ceres.binance.event.DiffBookEvent;
import com.blokaly.ceres.binance.event.OrderBookEvent;
import com.blokaly.ceres.chronicle.PayloadType;
import com.blokaly.ceres.chronicle.WriteStore;
import com.blokaly.ceres.network.RestGetJson;
import com.blokaly.ceres.orderbook.PriceBasedOrderBook;
import com.blokaly.ceres.orderbook.TopOfBookProcessor;
import com.blokaly.ceres.utils.EventQueueSpliterator;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;

public class OrderBookHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(OrderBookHandler.class);
  private static final String ORDER_BOOK_URL = "https://www.binance.com/api/v1/depth?symbol=%s&limit=1000";
  private final PriceBasedOrderBook orderBook;
  private final TopOfBookProcessor processor;
  private final Gson gson;
  private final WriteStore store;
  private final ExecutorService executorService;
  private final EventQueueSpliterator<DiffBookEvent> splitter;
  private final AtomicBoolean started;
  private final JsonParser parser;

  public OrderBookHandler(PriceBasedOrderBook orderBook,
                          TopOfBookProcessor processor,
                          Gson gson,
                          WriteStore store,
                          ExecutorService executorService) {
    this.orderBook = orderBook;
    this.processor = processor;
    this.gson = gson;
    this.store = store;
    this.executorService = executorService;
    splitter = new EventQueueSpliterator<>();
    started = new AtomicBoolean(false);
    this.parser = new JsonParser();
  }

  public String getSymbol() {
    return orderBook.getSymbol();
  }

  public void init() {
    executorService.execute(orderBook::clear);

    if (started.compareAndSet(false, true)) {
      executorService.execute(() -> {
        StreamSupport.stream(splitter, false).forEach(event -> {
          if (orderBook.isInitialized()) {
            if (event.getBeginSequence() <= orderBook.getLastSequence() + 1) {
              orderBook.processIncrementalUpdate(event.getDeletion());
              orderBook.processIncrementalUpdate(event.getUpdate());
            }
          } else {
            String symbol = orderBook.getSymbol();
            String url = String.format(ORDER_BOOK_URL, symbol.toUpperCase());
            String jsonResponse = RestGetJson.request(url);
            OrderBookEvent snapshot = gson.fromJson(jsonResponse, OrderBookEvent.class);
            LOGGER.debug("{}", snapshot);
            while (snapshot.getSequence() <= event.getEndSequence()) {
              try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
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
            store.save(PayloadType.JSON, jsonObj.toString());
            orderBook.processSnapshot(snapshot);
          }
          processor.process(orderBook);
        });
      });
    }
  }

  public void reset() {
    executorService.execute(()->{
      orderBook.clear();
      processor.process(orderBook);
    });
  }

  public void handle(DiffBookEvent event) {
    splitter.add(event);
  }
}
