package com.blokaly.ceres.bitmex;

import com.blokaly.ceres.bitmex.event.Trades;
import com.blokaly.ceres.data.OrderInfo;
import com.blokaly.ceres.influxdb.ringbuffer.BatchedPointsPublisher;
import com.blokaly.ceres.influxdb.ringbuffer.PointBuilderFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

@Singleton
public class TradesHandler {
  private static final String MEASUREMENT = "Trades";
  private static final String SYMBOL_COL = "symbol";
  private static final String SIDE_COL = "side";
  private static final String PRICE_COL = "price";
  private static final String SIZE_COL = "size";
  private final BatchedPointsPublisher publisher;

  @Inject
  public TradesHandler(BatchedPointsPublisher publisher) {
    this.publisher = publisher;
  }

  public void publishTrades(Collection<Trades.Trade> trades) {
    trades.forEach(trade -> {
      publisher.publish(builder -> {
        buildPoint(trade.getTime(), trade.getSymbol(), trade.side()== OrderInfo.Side.BUY ? "B" : "S", trade.getPrice().asDbl(), trade.getQuantity().asDbl(), builder);
      });
    });
  }

  private void buildPoint(long time, String symbol, String side, double price, double size, PointBuilderFactory.BatchedPointBuilder builder) {
    builder.measurement(MEASUREMENT).time(time, TimeUnit.MILLISECONDS);
    builder.tag(SYMBOL_COL, symbol.toUpperCase());
    builder.tag(SIDE_COL, side);
    builder.addField(PRICE_COL, price);
    builder.addField(SIZE_COL, size);
  }
}
