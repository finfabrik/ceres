package com.blokaly.ceres.bitmex.event;

import com.blokaly.ceres.common.DecimalNumber;
import com.blokaly.ceres.data.IdBasedOrderInfo;
import com.blokaly.ceres.data.MarketDataIncremental;
import com.blokaly.ceres.data.OrderInfo;
import com.blokaly.ceres.data.SymbolFormatter;
import com.google.gson.*;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Incremental implements MarketDataIncremental<IdBasedOrderInfo> {
  private final long sequence;
  private final String symbol;
  private final Type type;
  private final Collection<IdBasedOrderInfo> orderInfos;

  public Incremental(long sequence, JsonObject data) {
    this.sequence = sequence;
    this.type = parseType(data);
    JsonArray orders = data.get("data").getAsJsonArray();
    this.symbol = SymbolFormatter.normalise(orders.get(0).getAsJsonObject().get("symbol").getAsString());
    this.orderInfos = StreamSupport.stream(orders.spliterator(), false).map(elm -> new MDIncrementalOrderInfo(elm.getAsJsonObject())).collect(Collectors.toList());
  }

  public String getSymbol() {
    return symbol;
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public long getSequence() {
    return sequence;
  }

  @Override
  public Collection<IdBasedOrderInfo> orderInfos() {
    return orderInfos;
  }

  private static MarketDataIncremental.Type parseType(JsonObject data) {
    String action = data.get("action").getAsString();
    switch (action) {
      case "insert": return Type.NEW;
      case "update": return Type.UPDATE;
      case "delete": return Type.DONE;
      default: return Type.UNKNOWN;
    }
  }

  public static class Adapter implements JsonDeserializer<Incremental> {

    @Override
    public Incremental deserialize(JsonElement jsonElement, java.lang.reflect.Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
      return new Incremental(System.nanoTime(), jsonElement.getAsJsonObject());
    }
  }

  private class MDIncrementalOrderInfo implements IdBasedOrderInfo {
    private final JsonObject data;

    private MDIncrementalOrderInfo(JsonObject data) {
      this.data = data;
    }

    @Override
    public DecimalNumber getPrice() {
      return DecimalNumber.fromStr(data.get("price").getAsString());
    }

    @Override
    public DecimalNumber getQuantity() {
      return DecimalNumber.fromStr(data.get("size").getAsString());
    }

    @Override
    public String getId() {
      return data.get("id").getAsString();
    }

    @Override
    public Side side() {
      return OrderInfo.Side.BUY.name().equalsIgnoreCase(data.get("side").getAsString()) ? Side.BUY : Side.SELL;
    }

    @Override
    public String toString() {
      return data.toString();
    }
  }
}
