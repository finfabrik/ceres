package com.blokaly.ceres.binance.event;

import com.blokaly.ceres.data.MarketDataIncremental;
import com.blokaly.ceres.data.OrderInfo;
import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DiffBookEvent {

  private final long evtTime;
  private final long begin;
  private final long end;
  private final MarketDataIncremental<OrderInfo> update;
  private final MarketDataIncremental<OrderInfo> deletion;

  public static DiffBookEvent parse(long evtTime, long begin, long end, JsonArray bidArray, JsonArray askArray) {

    Map<Boolean, List<OrderInfo>> bids = StreamSupport.stream(bidArray.spliterator(), false)
        .map(elm -> new JsonArrayOrderInfo(OrderInfo.Side.BUY, elm.getAsJsonArray()))
        .collect(Collectors.partitioningBy(order -> order.getQuantity().isZero()));

    Map<Boolean, List<OrderInfo>> asks = StreamSupport.stream(askArray.spliterator(), false)
        .map(elm -> new JsonArrayOrderInfo(OrderInfo.Side.SELL, elm.getAsJsonArray()))
        .collect(Collectors.partitioningBy(order -> order.getQuantity().isZero()));

    RefreshEvent update = new RefreshEvent(end, MarketDataIncremental.Type.UPDATE);
    update.add(bids.get(false));
    update.add(asks.get(false));

    RefreshEvent deletion = new RefreshEvent(end, MarketDataIncremental.Type.DONE);
    deletion.add(bids.get(true));
    deletion.add(asks.get(true));

    return new DiffBookEvent(evtTime, begin, end, update, deletion);
  }

  public DiffBookEvent(long time, long begin, long end, MarketDataIncremental<OrderInfo> update, MarketDataIncremental<OrderInfo> deletion) {
    this.evtTime = time;
    this.begin = begin;
    this.end = end;
    this.update = update;
    this.deletion = deletion;
  }

  public long getEventTime() {
    return evtTime;
  }

  public long getBeginSequence() {
    return begin;
  }

  public long getEndSequence() {
    return end;
  }

  public MarketDataIncremental<OrderInfo> getUpdate() {
    return update;
  }

  public MarketDataIncremental<OrderInfo> getDeletion() {
    return deletion;
  }

  public static class Adapter implements JsonDeserializer<DiffBookEvent> {

    @Override
    public DiffBookEvent deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

      JsonObject jsonObject = json.getAsJsonObject();
      long evtTime = jsonObject.get("E").getAsLong();
      long begin = jsonObject.get("U").getAsLong();
      long end = jsonObject.get("u").getAsLong();
      JsonArray bids = jsonObject.get("b").getAsJsonArray();
      JsonArray asks = jsonObject.get("a").getAsJsonArray();
      return DiffBookEvent.parse(evtTime, begin, end, bids, asks);
    }
  }
}
