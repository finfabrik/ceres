package com.blokaly.ceres.hitbtc.data;

import com.blokaly.ceres.hitbtc.event.EventType;
import com.google.gson.JsonArray;

import java.util.Collection;
import java.util.List;

public class TradesSnapshot extends Trades {


  public TradesSnapshot(String symbol, Collection<TradeRecord> trades, long receivedTime) {
    super(EventType.TRADES_SNAPSHOT, symbol, trades, receivedTime);
  }

  public static TradesSnapshot parse(String symbol, JsonArray trades, long receivedTime){
    List<TradeRecord> tradeRecords = getTradeRecords(trades);
    return new TradesSnapshot(symbol, tradeRecords, receivedTime);
  }

}
