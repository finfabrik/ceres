package com.blokaly.ceres.bitmex.event;

import com.blokaly.ceres.common.DecimalNumber;
import com.blokaly.ceres.data.OrderInfo;
import com.google.gson.*;

import java.lang.reflect.Type;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Trades {
  private final long recTime;
  private final long sequence;
  private final Collection<Trade> trades;

  private Trades(long time, long sequence, Collection<Trade> trades) {
    this.recTime = time;
    this.sequence = sequence;
    this.trades = trades;
  }

  public long getTime() {
    return recTime;
  }

  public long getSequence() {
    return sequence;
  }

  public Collection<Trade> getTrades() {
    return trades;
  }

  public static class Adapter implements JsonDeserializer<Trades> {

    @Override
    public Trades deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
      JsonObject event = jsonElement.getAsJsonObject();
      String action = event.get("action").getAsString().toLowerCase();
      switch (action) {
        case "partial":
        case "insert": break;
        default: throw new UnsupportedOperationException("Only support 'insert' action");
      }

      JsonArray data = event.get("data").getAsJsonArray();
      List<Trade> trades = StreamSupport.stream(data.spliterator(), false)
          .map(elm -> new Trade(elm.getAsJsonObject()))
          .collect(Collectors.toList());

      return new Trades(System.currentTimeMillis(), System.nanoTime(), trades);
    }
  }

  public static class Trade implements OrderInfo{
    private final JsonObject data;
    private final ZonedDateTime utcTime;

    private Trade(JsonObject elm) {
      data = elm;
      utcTime = ZonedDateTime.parse(elm.get("timestamp").getAsString());
    }

    public long getTime() {
      return utcTime.toInstant().toEpochMilli();
    }

    public String getSymbol() {
      return data.get("symbol").getAsString();
    }

    @Override
    public Side side() {
      return "Buy".equalsIgnoreCase(data.get("side").getAsString()) ? Side.BUY : Side.SELL;
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
    public String toString() {
      return data.toString();
    }
  }
}
