package com.blokaly.ceres.hitbtc;

import com.blokaly.ceres.common.PairSymbol;
import com.blokaly.ceres.data.OrderInfo;
import com.blokaly.ceres.hitbtc.data.Trades;
import com.blokaly.ceres.hitbtc.event.TradeSRequestEvent;
import com.blokaly.ceres.influxdb.ringbuffer.BatchedPointsPublisher;
import com.blokaly.ceres.influxdb.ringbuffer.PointBuilderFactory;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TradesHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(TradesHandler.class);
  private static final String MEASUREMENT = "Trades";
  private static final String SYMBOL_COL = "symbol";
  private static final String PRICE_COL = "price";
  private static final String SIZE_COL = "size";
  private static final String SIDE_COL = "side";
  private static final String TRADE_ID_COL = "tradeId";
  private static final String RECEIVED_TS_COL = "receivedTime";
  private static final String NULL_STRING = "-";
  private final Map<String, PairSymbol> symbols;
  private final BatchedPointsPublisher publisher;

  @Inject
  public TradesHandler(@Named("SymbolMap") Map<String, PairSymbol> symbols,
                       BatchedPointsPublisher publisher) {
    this.symbols = symbols;
    this.publisher = publisher;
  }

  public void subscribeAll(HitbtcClient sender, Gson gson) {
    symbols.values().forEach(pair -> {
      String sym = pair.getCode().toUpperCase();
      try {
        String jsonString = gson.toJson(new TradeSRequestEvent(sym, System.nanoTime()));
        LOGGER.info("Subscribing to trades[{}]...", sym);
        sender.send(jsonString);
      } catch (Exception e) {
        LOGGER.error("Error subscribing to trades " + sym, e);
      }
    });
  }

  public void publishTrades(Trades trades) {

    PairSymbol pair = symbols.get(trades.getSymbol().toLowerCase());
    if (pair!=null) {
      long recTime = trades.getRecTime();
      trades.getTrades().forEach(trade -> {
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
        buildPoint(now, pair.toPairString(), 0D, 0D, NULL_STRING, Integer.MIN_VALUE, now, builder);
      });
    });

  }

  private void buildPoint(long time, String symbol, double price, double size, String side, long tradeId, long recTime, PointBuilderFactory.BatchedPointBuilder builder) {
    builder.measurement(MEASUREMENT).time(time, TimeUnit.MILLISECONDS);
    builder.tag(SYMBOL_COL, symbol.toUpperCase());
    builder.addField(PRICE_COL, price);
    builder.addField(SIZE_COL, size);
    builder.addField(SIDE_COL, side);
    if (tradeId >= 0) {
      builder.addField(TRADE_ID_COL, tradeId);
      builder.addField(RECEIVED_TS_COL, recTime);
    }
  }
}
