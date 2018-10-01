package com.blokaly.ceres.hitbtc.data;

import com.blokaly.ceres.hitbtc.event.EventType;
import com.google.gson.JsonArray;

import java.util.Collection;
import java.util.List;

public class TradesUpdate extends Trades {


  public TradesUpdate(String symbol, Collection<TradeRecord> trades, long receivedTime) {
    super(EventType.TRADES_UPDATE, symbol, trades, receivedTime);
  }

  public static TradesUpdate parse(String symbol, JsonArray trades, long receivedTime ){
    List<TradeRecord> tradeRecords = getTradeRecords(trades);
    return new TradesUpdate(symbol, tradeRecords, receivedTime);

  }
}
