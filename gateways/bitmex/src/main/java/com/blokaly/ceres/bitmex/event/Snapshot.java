package com.blokaly.ceres.bitmex.event;

import com.blokaly.ceres.common.DecimalNumber;
import com.blokaly.ceres.data.IdBasedOrderInfo;
import com.blokaly.ceres.data.MarketDataSnapshot;
import com.blokaly.ceres.data.OrderInfo;
import com.blokaly.ceres.data.SymbolFormatter;
import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Snapshot implements MarketDataSnapshot<IdBasedOrderInfo> {
  private final long recTime;
  private final String symbol;
  private final long sequence;
  private final Collection<IdBasedOrderInfo> bids;
  private final Collection<IdBasedOrderInfo> asks;

  private Snapshot(long time, String symbol, long sequence, Collection<IdBasedOrderInfo> bids, Collection<IdBasedOrderInfo> asks) {
    this.recTime = time;
    this.symbol = symbol;
    this.sequence = sequence;
    this.bids = bids;
    this.asks = asks;
  }

  public long getTime() {
    return recTime;
  }

  public String getSymbol() {
    return symbol;
  }

  @Override
  public long getSequence() {
    return sequence;
  }

  @Override
  public Collection<IdBasedOrderInfo> getBids() {
    return bids;
  }

  @Override
  public Collection<IdBasedOrderInfo> getAsks() {
    return asks;
  }

  public static class Adapter implements JsonDeserializer<Snapshot> {

    @Override
    public Snapshot deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

      JsonObject jsonObject = json.getAsJsonObject();
      JsonArray data = jsonObject.has("data") ? jsonObject.get("data").getAsJsonArray() : null;

      if (data == null || data.size()==0) {
        return new Snapshot(0, "", 0, Collections.emptyList(), Collections.emptyList());
      }

      Map<Boolean, List<IdBasedOrderInfo>> bidAndAsk = StreamSupport.stream(data.spliterator(), false)
          .map(elm -> new SnapshotOrderInfo(elm.getAsJsonObject()))
          .collect(Collectors.partitioningBy(snapshotOrderInfo -> snapshotOrderInfo.side() == OrderInfo.Side.BUY));

      String symbol = data.get(0).getAsJsonObject().get("symbol").getAsString();
      return new Snapshot(System.currentTimeMillis(), SymbolFormatter.normalise(symbol), System.nanoTime(), bidAndAsk.get(true), bidAndAsk.get(false));
    }
  }

  private static class SnapshotOrderInfo implements IdBasedOrderInfo {
    private final Side side;
    private final JsonObject json;

    private SnapshotOrderInfo(JsonObject json) {
      this.json = json;
      this.side = OrderInfo.Side.BUY.name().equalsIgnoreCase(json.get("side").getAsString()) ? Side.BUY : Side.SELL;
    }

    @Override
    public String getId() {
      return json.get("id").getAsString();
    }

    @Override
    public Side side() {
      return side;
    }

    @Override
    public DecimalNumber getPrice() {
      return DecimalNumber.fromStr(json.get("price").getAsString());
    }

    @Override
    public DecimalNumber getQuantity() {
      return DecimalNumber.fromStr(json.get("size").getAsString());
    }
  }
}
