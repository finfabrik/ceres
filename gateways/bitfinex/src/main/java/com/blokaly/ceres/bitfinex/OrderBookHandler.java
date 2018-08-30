package com.blokaly.ceres.bitfinex;

import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.bitfinex.event.OrderBookRefresh;
import com.blokaly.ceres.bitfinex.event.OrderBookSnapshot;
import com.blokaly.ceres.common.PairSymbol;
import com.blokaly.ceres.common.Source;
import com.blokaly.ceres.data.MarketDataIncremental;
import com.blokaly.ceres.data.OrderInfo;
import com.blokaly.ceres.data.SymbolFormatter;
import com.blokaly.ceres.influxdb.ringbuffer.BatchedPointsPublisher;
import com.blokaly.ceres.influxdb.ringbuffer.PointBuilderFactory;
import com.blokaly.ceres.orderbook.OrderBasedOrderBook;
import com.blokaly.ceres.orderbook.OrderBook;
import com.blokaly.ceres.orderbook.PriceBasedOrderBook;
import com.blokaly.ceres.orderbook.TopOfBookProcessor;
import com.blokaly.ceres.system.CommonConfigs;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final Map<Integer, OrderBasedOrderBook> orderbooks;
    private final Map<Integer, String> symMap;
    private final Map<String, PairSymbol> symbols;
    private final String source;
    private final TopOfBookProcessor tobProcessor;
    private final BatchedPointsPublisher publisher;
    private final ScheduledExecutorService ses;

    @Inject
    public OrderBookHandler(Config config, TopOfBookProcessor tobProcessor, BatchedPointsPublisher publisher, @SingleThread ScheduledExecutorService ses) {
        source = Source.valueOf(config.getString(CommonConfigs.APP_SOURCE).toUpperCase()).getCode();
        this.tobProcessor = tobProcessor;
        this.publisher = publisher;
        this.ses = ses;
        symbols = config.getStringList("symbols").stream().collect(Collectors.toMap(SymbolFormatter::normalise, PairSymbol::parse));
        orderbooks = Maps.newHashMap();
        symMap = Maps.newHashMap();
    }

    public Collection<String> getAllSymbols() {
        return symbols.keySet();
    }

    public String getSymbol(int channel) {
        return symMap.get(channel);
    }

    public void makeOrderBook(int channel, String symbol) {
        OrderBasedOrderBook book = orderbooks.get(channel);
        symbol = SymbolFormatter.normalise(symbol);
        String key = symbol + "." + source;
        if (book == null) {
            book = new OrderBasedOrderBook(symbol, key);
            orderbooks.put(channel, book);
            symMap.put(channel, symbol);
        } else {
            book.clear();
            if (!book.getSymbol().equals(symbol)) {
                book = new OrderBasedOrderBook(symbol, key);
                orderbooks.put(channel, book);
            }
        }
    }

    public Collection<OrderBasedOrderBook> getAllBooks() {
        return orderbooks.values();
    }

    public OrderBasedOrderBook get(int channel) {
        return orderbooks.get(channel);
    }

    public void reset() {
        ses.execute(()->{
            orderbooks.values().forEach(book ->{
                book.clear();
                tobProcessor.process(book);
            });
        });
    }

    public void processSnapshot(OrderBookSnapshot snapshot) {

        OrderBasedOrderBook book = orderbooks.get(snapshot.getChannelId());
        if (book == null) {
            return;
        }

        ses.execute(()->{
            book.processSnapshot(snapshot);
            publishBook(snapshot.getTime(), book);
        });

        tobProcessor.process(book);
    }


    public void processIncremental(OrderBookRefresh refresh) {
        OrderBasedOrderBook book = orderbooks.get(refresh.getChannelId());
        if (book == null) {
            return;
        }

        ses.execute(()->{
            book.processIncrementalUpdate(refresh);
            publishDelta(refresh.getTime(), book);
        });

        tobProcessor.process(book);
    }

    public void publishOpen() {
        symbols.values().forEach(symbol -> {
            publisher.publish(builder -> {
                buildPoint(System.currentTimeMillis(), symbol.toPairString(), NULL_STRING, "S", "0", 0D, 0D, builder);
            });
        });
    }

    private void publishBook(long time, OrderBasedOrderBook book) {
        if (book.getLastSequence() <= 0) {
            return;
        }

        String symbol = symbols.get(book.getSymbol()).toPairString();
        LOGGER.info("Storing orderbook snapshot for {}", symbol);

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

    private void buildPoint(long time, String symbol, String side, String action, String intraTs, double price, double size, PointBuilderFactory.BatchedPointBuilder builder) {
        builder.measurement(MEASUREMENT).time(time, TimeUnit.MILLISECONDS);
        builder.tag(SYMBOL_COL, symbol.toUpperCase());
        builder.tag(SIDE_COL, side);
        builder.tag(ACTION_COL, action);
        builder.tag(INTRA_TS_COL, intraTs);
        builder.addField(PRICE_COL, price);
        builder.addField(SIZE_COL, size);
    }

    private void publishDelta(long time, OrderBasedOrderBook book) {
        String symbol = symbols.get(book.getSymbol()).toPairString();
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
