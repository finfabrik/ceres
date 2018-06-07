package com.blokaly.ceres.binance;

import com.blokaly.ceres.binance.event.DiffBookEvent;
import com.blokaly.ceres.binance.event.OrderBookEvent;
import com.blokaly.ceres.kafka.ToBProducer;
import com.blokaly.ceres.orderbook.PriceBasedOrderBook;
import com.blokaly.ceres.utils.EventQueueSpliterator;
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
    private final ToBProducer producer;
    private final Gson gson;
    private final ExecutorService executorService;
    private final EventQueueSpliterator<DiffBookEvent> splitter;
    private final AtomicBoolean started;

    public OrderBookHandler(PriceBasedOrderBook orderBook, ToBProducer producer, Gson gson, ExecutorService executorService) {
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
                        OrderBookSnapshotRequester requester = new OrderBookSnapshotRequester(orderBook.getSymbol());
                        OrderBookEvent snapshot = gson.fromJson(requester.request(), OrderBookEvent.class);
                        LOGGER.debug("{}", snapshot);
                        while (snapshot.getSequence() <= event.getEndSequence()) {
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
                    producer.publish(orderBook);
                });
            });
        }
    }

    public void reset() {
        executorService.execute(()->{
            orderBook.clear();
            producer.publish(orderBook);
        });
    }

    public void handle(DiffBookEvent event) {
        splitter.add(event);
    }
}
