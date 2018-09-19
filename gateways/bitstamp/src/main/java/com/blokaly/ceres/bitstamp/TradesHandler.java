package com.blokaly.ceres.bitstamp;

import com.blokaly.ceres.bitstamp.event.TradeEvent;
import com.blokaly.ceres.influxdb.ringbuffer.BatchedPointsPublisher;
import com.blokaly.ceres.influxdb.ringbuffer.PointBuilderFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.concurrent.TimeUnit;

@Singleton
public class TradesHandler {
  private static final String MEASUREMENT = "Trades";
  private static final String SYMBOL_COL = "symbol";
  private static final String PRICE_COL = "price";
  private static final String SIZE_COL = "size";
  private static final String TRADE_ID_COL = "tradeId";
  private static final String SIDE_COL = "side";
  private static final String TRADE_TS_COL = "tradeTime";
  private static final String INTRA_TS_COL = "intraTimestampTag";
  private static final String NULL_STRING = "-";
  private final BatchedPointsPublisher publisher;


  @Inject
  public TradesHandler(BatchedPointsPublisher publisher) {
    this.publisher = publisher;
  }

  public void publishTrades(String pair, TradeEvent trade) {
    publisher.publish(builder -> {
      buildPoint(trade.getRecTime(), pair, trade.getSide(), trade.getPrice().asDbl(), trade.getQuantity().asDbl(), trade.getTradeId(), trade.getTradeTime(), builder);
    });
  }

  public void publishOpen(long openTime, String pair) {
    publisher.publish(builder -> {
      buildPoint(openTime, pair, NULL_STRING, 0D, 0D, 0, 0, builder);
    });
  }

  private void buildPoint(long time, String symbol, String side, double price, double size, long tradeId, long tradeTime, PointBuilderFactory.BatchedPointBuilder builder) {
    builder.measurement(MEASUREMENT).time(time, TimeUnit.NANOSECONDS);
    builder.tag(SYMBOL_COL, symbol.toUpperCase());
    builder.tag(INTRA_TS_COL, "0");
    builder.addField(PRICE_COL, price);
    builder.addField(SIZE_COL, size);
    builder.addField(SIDE_COL, side);
    builder.addField(TRADE_ID_COL, tradeId);
    builder.addField(TRADE_TS_COL, tradeTime);
  }
}
