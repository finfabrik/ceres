package com.blokaly.ceres.binance;

import com.blokaly.ceres.binance.event.TradeEvent;
import com.blokaly.ceres.common.PairSymbol;
import com.blokaly.ceres.influxdb.ringbuffer.BatchedPointsPublisher;
import com.blokaly.ceres.influxdb.ringbuffer.PointBuilderFactory;
import com.google.inject.Inject;

import java.util.concurrent.TimeUnit;

public class TradesHandler {
  private static final String MEASUREMENT = "Trades";
  private static final String SYMBOL_COL = "symbol";
  private static final String PRICE_COL = "price";
  private static final String SIZE_COL = "size";
  private static final String TRADE_TIME_COL = "tradeTime";
  private static final String TRADE_ID_COL = "tradeId";
  private static final String MAKER_COL = "buyerIsMaker";
  private final PairSymbol pair;
  private final BatchedPointsPublisher publisher;

  @Inject
  public TradesHandler(PairSymbol pair, BatchedPointsPublisher publisher) {
    this.pair = pair;
    this.publisher = publisher;
  }

  public void publishTrades(TradeEvent trade) {
    if (pair.getCode().equalsIgnoreCase(trade.getSymbol())) {
      publisher.publish(builder -> {
        String symbol = pair.toPairString();
        buildPoint(trade.getTime(), symbol, trade.getPrice().asDbl(), trade.getQuantity().asDbl(), trade.getTradeTime(), trade.getTradeId(), trade.isBuyerMarketMaker(), builder);
      });
    }
  }

  public void publishOpen() {
    publisher.publish(builder -> {
      buildPoint(System.currentTimeMillis(), pair.toPairString(), 0D, 0D, 0L, 0L, false, builder);
    });
  }

  private void buildPoint(long time, String symbol, double price, double size, long tradeTime, long tradeId, boolean isBuyerMaker, PointBuilderFactory.BatchedPointBuilder builder) {
    builder.measurement(MEASUREMENT).time(time, TimeUnit.MILLISECONDS);
    builder.tag(SYMBOL_COL, symbol.toUpperCase());
    builder.addField(PRICE_COL, price);
    builder.addField(SIZE_COL, size);
    builder.addField(TRADE_TIME_COL, tradeTime);
    builder.addField(TRADE_ID_COL, tradeId);
    builder.addField(MAKER_COL, isBuyerMaker);
  }
}
