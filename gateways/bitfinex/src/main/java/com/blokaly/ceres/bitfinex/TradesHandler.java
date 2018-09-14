package com.blokaly.ceres.bitfinex;

import com.blokaly.ceres.bitfinex.event.SubscriptionEvent;
import com.blokaly.ceres.bitfinex.event.TradeEvent;
import com.blokaly.ceres.common.PairSymbol;
import com.blokaly.ceres.data.OrderInfo;
import com.blokaly.ceres.influxdb.ringbuffer.BatchedPointsPublisher;
import com.blokaly.ceres.influxdb.ringbuffer.PointBuilderFactory;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class TradesHandler {
  private static final String MEASUREMENT = "Trades";
  private static final String SYMBOL_COL = "symbol";
  private static final String PRICE_COL = "price";
  private static final String SIZE_COL = "size";
  private static final String SIDE_COL = "side";
  private static final String TRADE_ID_COL = "tradeId";
  private static final String RECEIVED_TS_COL = "receivedTime";
  private static final String NULL_STRING = "-";
  private final Map<String, PairSymbol> symbols;
  private final ConcurrentMap<Integer, SubscriptionEvent> channelMap;
  private final BatchedPointsPublisher publisher;

  @Inject
  public TradesHandler(@Named("SymbolMap") Map<String, PairSymbol> symbols,
                       @Named("ChannelMap") ConcurrentMap<Integer, SubscriptionEvent> channelMap,
                       BatchedPointsPublisher publisher) {
    this.symbols = symbols;
    this.channelMap = channelMap;
    this.publisher = publisher;
  }

  public void publishTrades(TradeEvent evt) {

    SubscriptionEvent sub = channelMap.get(evt.getChannelId());
    if (sub == null) {
      return;
    }

    PairSymbol pair = symbols.get(sub.getPair().toLowerCase());
    if (pair!=null) {
      long recTime = evt.getRecTime();
      evt.getTrades().forEach(trade -> {
        publisher.publish(builder -> {
          String side = trade.getSide() == OrderInfo.Side.BUY ? "B" : "S";
          buildPoint(trade.getTime(), pair.toPairString(), trade.getPrice().asDbl(), trade.getQuantity().asDbl(), side, trade.getTradeId(), recTime, builder);
        });
      });
    }
  }

  public void publishOpen() {
    symbols.values().forEach(pair -> {
      publisher.publish(builder -> {
        long now = System.currentTimeMillis();
        buildPoint(TimeUnit.MILLISECONDS.toSeconds(now), pair.toPairString(), 0D, 0D, NULL_STRING, 0L, now, builder);
      });
    });

  }

  private void buildPoint(long time, String symbol, double price, double size, String side, long tradeId, long recTime, PointBuilderFactory.BatchedPointBuilder builder) {
    builder.measurement(MEASUREMENT).time(time, TimeUnit.SECONDS);
    builder.tag(SYMBOL_COL, symbol.toUpperCase());
    builder.addField(PRICE_COL, price);
    builder.addField(SIZE_COL, size);
    builder.addField(SIDE_COL, side);
    builder.addField(TRADE_ID_COL, tradeId);
    builder.addField(RECEIVED_TS_COL, recTime);
  }

}
