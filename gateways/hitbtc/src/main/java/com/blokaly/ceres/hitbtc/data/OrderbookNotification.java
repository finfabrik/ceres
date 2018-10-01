package com.blokaly.ceres.hitbtc.data;

import com.blokaly.ceres.common.DecimalNumber;
import com.blokaly.ceres.data.MarketDataIncremental;
import com.blokaly.ceres.data.OrderInfo;
import com.blokaly.ceres.hitbtc.event.AbstractEvent;
import com.blokaly.ceres.hitbtc.event.EventType;
import com.google.gson.JsonArray;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class OrderbookNotification extends AbstractEvent {
  private final String symbol;
  private final long sequence;
  private final MarketDataIncrementalTagged update;
  private final MarketDataIncrementalTagged deletion;
  private final long receivedTime;

  public OrderbookNotification(final String symbol, final long sequence, MarketDataIncrementalTagged update, MarketDataIncrementalTagged deletion, long receivedTime) {
    super(EventType.ORDERBOOK_UPDATE.getType());
    this.sequence = sequence;
    this.symbol = symbol;
    this.update = update;
    this.deletion = deletion;
    this.receivedTime = receivedTime;
  }

  public static OrderbookNotification parse(String symbol, long sequence, JsonArray asksArray, JsonArray bidsArray, long receivedTime) {
    Map<Boolean, List<OrderInfo>> bids = StreamSupport.stream(bidsArray.spliterator(), false)
        .map(elm -> new OrderRecord(DecimalNumber.fromBD(elm.getAsJsonObject().get("price").getAsBigDecimal()), DecimalNumber.fromBD(elm.getAsJsonObject().get("size").getAsBigDecimal()), OrderInfo.Side.BUY))
        .collect(Collectors.partitioningBy(order -> order.getQuantity().isZero()));

    Map<Boolean, List<OrderInfo>> asks = StreamSupport.stream(asksArray.spliterator(), false)
        .map(elm -> new OrderRecord(DecimalNumber.fromBD(elm.getAsJsonObject().get("price").getAsBigDecimal()), DecimalNumber.fromBD(elm.getAsJsonObject().get("size").getAsBigDecimal()), OrderInfo.Side.SELL))
        .collect(Collectors.partitioningBy(order -> order.getQuantity().isZero()));

    MarketDataIncrementalTagged update = new MarketDataIncrementalTagged(sequence, MarketDataIncremental.Type.UPDATE);
    update.add(bids.get(false));
    update.add(asks.get(false));

    MarketDataIncrementalTagged deletion = new MarketDataIncrementalTagged(sequence, MarketDataIncremental.Type.DONE);
    deletion.add(bids.get(true));
    deletion.add(asks.get(true));

    return new OrderbookNotification(symbol, sequence, update, deletion, receivedTime);

  }

  public MarketDataIncremental<OrderInfo> getUpdate() {
    return update;
  }

  public MarketDataIncremental<OrderInfo> getDeletion() {
    return deletion;
  }

  public long getSequence() {
    return sequence;
  }

  public String getSymbol() {
    return symbol;
  }

  public long getTime() {
    return receivedTime;
  }

  @Override
  public String toString() {
    return "OrderbookNotification={" +
        "sequence:" + sequence +
        ",symbol:" + symbol +
        ",updates:" + update +
        ",deletes:" + deletion + "}";

  }
}
