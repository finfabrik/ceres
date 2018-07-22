package com.blokaly.ceres.coinmarketcap;

import com.blokaly.ceres.common.DecimalNumber;
import com.google.gson.*;

import java.lang.reflect.Type;

public class TickerEvent {
    private final String symbol;
    private final DecimalNumber usdPrice;
    private final long lastUpdate;

  public TickerEvent(String symbol, DecimalNumber usdPrice, long lastUpdate) {
    this.symbol = symbol;
    this.usdPrice = usdPrice;
    this.lastUpdate = lastUpdate;
  }

  public String getSymbol() {
    return symbol;
  }

  public DecimalNumber getUsdPrice() {
    return usdPrice;
  }

  public boolean isValid() {
    return !usdPrice.isZero();
  }

  public static class EventAdapter implements JsonDeserializer<TickerEvent> {
    @Override
    public TickerEvent deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
      JsonObject tickerJson = json.getAsJsonObject();
      String symbol = tickerJson.get("symbol").getAsString();
      JsonElement priceElm = tickerJson.get("price_usd");
      DecimalNumber priceUsd = priceElm.isJsonNull() ? DecimalNumber.ZERO : DecimalNumber.fromStr(priceElm.getAsString());
      JsonElement lastUpdateElm = tickerJson.get("last_updated");
      long lastUpdate = lastUpdateElm.isJsonNull() ?  0L : lastUpdateElm.getAsLong();
      return new TickerEvent(symbol, priceUsd, lastUpdate);
    }
  }
}
