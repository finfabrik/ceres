package com.blokaly.ceres.bitstamp;

import com.blokaly.ceres.bitstamp.event.DiffBookEvent;
import com.blokaly.ceres.bitstamp.event.OrderBookEvent;
import com.blokaly.ceres.orderbook.PriceBasedOrderBook;
import com.blokaly.ceres.orderbook.TopOfBookProcessor;
import com.blokaly.ceres.utils.EventQueueSpliterator;
import com.blokaly.ceres.utils.StreamEvent;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;

public class OrderBookHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(OrderBookHandler.class);
  private final PriceBasedOrderBook orderBook;
  private final TopOfBookProcessor producer;
  private final Gson gson;
  private final ExecutorService executorService;
  private final EventQueueSpliterator<StreamEvent> splitter;
  private final AtomicBoolean started;

  public OrderBookHandler(PriceBasedOrderBook orderBook, TopOfBookProcessor producer, Gson gson, ExecutorService executorService) {
    this.orderBook = orderBook;
    this.producer = producer;
    this.gson = gson;
    this.executorService = executorService;
    splitter = new EventQueueSpliterator<>();
    started = new AtomicBoolean(false);
  }

  public String getSymbol() {
    return orderBook.getSymbol();
  }

  public void start() {
    executorService.execute(orderBook::clear);
    if (started.compareAndSet(false, true)) {
      executorService.execute(() -> {
        StreamSupport.stream(splitter, false).forEach(event -> {
          if (event == StreamEvent.RESET) {
            orderBook.clear();
          } else {
            DiffBookEvent evt = (DiffBookEvent) event;
            if (orderBook.isInitialized()) {
              orderBook.processIncrementalUpdate(evt.getDeletion());
              orderBook.processIncrementalUpdate(evt.getUpdate());
            } else {
              OrderBookSnapshotRequester requester = new OrderBookSnapshotRequester(orderBook.getSymbol());
              OrderBookEvent snapshot = gson.fromJson(requester.request(), OrderBookEvent.class);
              while (snapshot.getSequence() <= evt.getSequence()) {
                try {
                  Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                  snapshot = gson.fromJson(requester.request(), OrderBookEvent.class);
                } catch (InterruptedException e) {
                  if (Thread.currentThread().isInterrupted()) {
                    LOGGER.info("Retrieving snapshot interrupted, quitting...");
                    break;
                  }
                }
              }
              orderBook.processSnapshot(snapshot);
            }
          }
          producer.process(orderBook);
        });
      });
    }
  }

  public void handle(DiffBookEvent event) {
    splitter.add(event);
  }
}
