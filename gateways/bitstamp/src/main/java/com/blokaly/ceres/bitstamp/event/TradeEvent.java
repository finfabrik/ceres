package com.blokaly.ceres.bitstamp.event;

import com.blokaly.ceres.common.DecimalNumber;

import java.util.StringJoiner;

public class TradeEvent {
  private long id;
  private String price_str;
  private String amount_str;
  private int type;
  private String timestamp;
  private long recTime;

  public void setRecTime(long recTime) {
    this.recTime = recTime;
  }

  public long getTradeTime() {
    return Long.parseLong(timestamp);
  }

  public long getTradeId() {
    return id;
  }

  public String getSide() {
    return type==0 ? "B" : "S";
  }

  public DecimalNumber getPrice() {
    return DecimalNumber.fromStr(price_str);
  }

  public DecimalNumber getQuantity() {
    return DecimalNumber.fromStr(amount_str);
  }

  public long getRecTime() {
    return recTime;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", TradeEvent.class.getSimpleName() + "[", "]")
        .add("id=" + id)
        .add("price='" + price_str + "'")
        .add("amount='" + amount_str + "'")
        .add("type=" + type)
        .add("timestamp='" + timestamp + "'")
        .toString();
  }
}
