package com.blokaly.ceres.binance.event;

import com.blokaly.ceres.common.DecimalNumber;
import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.StringJoiner;

public class TradeEvent {

  private final long evtTime;
  private final String symbol;
  private final long tradeId;
  private final DecimalNumber price;
  private final DecimalNumber quantity;
  private final long tradeTime;
  private final boolean buyerMaker;

  public TradeEvent(long evtTime, String symbol, long tradeId, DecimalNumber price, DecimalNumber quantity, long tradeTime, boolean buyerMaker) {
    this.evtTime = evtTime;
    this.symbol = symbol;
    this.tradeId = tradeId;
    this.price = price;
    this.quantity = quantity;
    this.tradeTime = tradeTime;
    this.buyerMaker = buyerMaker;
  }

  public long getTime() {
    return evtTime;
  }

  public String getSymbol() {
    return symbol;
  }

  public long getTradeId() {
    return tradeId;
  }

  public DecimalNumber getPrice() {
    return price;
  }

  public DecimalNumber getQuantity() {
    return quantity;
  }

  public long getTradeTime() {
    return tradeTime;
  }

  public boolean isBuyerMarketMaker() {
    return buyerMaker;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "[", "]")
        .add("evtTime=" + evtTime)
        .add("symbol=" + symbol)
        .add("price=" + price)
        .add("quantity=" + quantity)
        .toString();
  }

  public static class Adapter implements JsonDeserializer<TradeEvent> {

    @Override
    public TradeEvent deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

      JsonObject jsonObject = json.getAsJsonObject();
      long evtTime = jsonObject.get("E").getAsLong();
      String symbol = jsonObject.get("s").getAsString();
      long tradeId = jsonObject.get("t").getAsLong();
      DecimalNumber price = DecimalNumber.fromStr(jsonObject.get("p").getAsString());
      DecimalNumber quantity = DecimalNumber.fromStr(jsonObject.get("q").getAsString());
      long tradeTime = jsonObject.get("T").getAsLong();
      boolean buyerMaker = jsonObject.get("m").getAsBoolean();
      return new TradeEvent(evtTime, symbol, tradeId, price, quantity, tradeTime, buyerMaker);
    }
  }
}
