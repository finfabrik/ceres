package com.blokaly.ceres.hitbtc.data;

import com.blokaly.ceres.common.DecimalNumber;
import com.blokaly.ceres.data.OrderInfo;
import com.blokaly.ceres.hitbtc.event.AbstractEvent;
import com.blokaly.ceres.hitbtc.event.EventType;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.time.temporal.ChronoField.*;

public class Trades extends AbstractEvent {

  private static final DateTimeFormatter TIMESTAMP_FORMATTER;
  static {
    TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral('T')
        .appendValue(HOUR_OF_DAY, 2).appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2).appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2)
        .appendFraction(MILLI_OF_SECOND, 0, 3, true)
        .appendLiteral('Z')
        .toFormatter();
  }

  private final String symbol;
  private final Collection<TradeRecord> trades;
  private final long receivedTime;

  public Trades(EventType type, String symbol, Collection<TradeRecord> trades, long receivedTime) {
    super(type.getType());
    this.symbol = symbol;
    this.trades = trades;
    this.receivedTime = receivedTime;
  }

  public String getSymbol() {
    return symbol;
  }

  public Collection<TradeRecord> getTrades() {
    return trades;
  }

  public long getRecTime() {
    return receivedTime;
  }

  protected static List<TradeRecord> getTradeRecords(JsonArray trades) {
    return StreamSupport.stream(trades.spliterator(), false)
          .map(elm -> new TradeRecord(elm.getAsJsonObject()))
          .collect(Collectors.toList());
  }

  public static class TradeRecord {
    private final long id;
    private final DecimalNumber price;
    private final DecimalNumber quantity;
    private final OrderInfo.Side side;
    private final LocalDateTime timestamp;

    public TradeRecord(long id, DecimalNumber price, DecimalNumber quantity, OrderInfo.Side side, LocalDateTime timestamp) {
      this.id = id;
      this.price = price;
      this.quantity = quantity;
      this.side = side;
      this.timestamp = timestamp;
    }

    public TradeRecord(JsonObject trade) {
      long id = trade.get("id").getAsLong();
      DecimalNumber price = DecimalNumber.fromStr(trade.get("price").getAsString());
      DecimalNumber quantity = DecimalNumber.fromStr(trade.get("quantity").getAsString());
      OrderInfo.Side side = OrderInfo.Side.parse(trade.get("side").getAsString());
      LocalDateTime timestamp = LocalDateTime.parse(trade.get("timestamp").getAsString(), TIMESTAMP_FORMATTER);
      this.id = id;
      this.price = price;
      this.quantity = quantity;
      this.side = side;
      this.timestamp = timestamp;
    }

    public long getTradeId() {
      return id;
    }

    public DecimalNumber getPrice() {
      return price;
    }

    public DecimalNumber getQuantity() {
      return quantity;
    }

    public OrderInfo.Side getSide() {
      return side;
    }

    public long getTime() {
      return timestamp.toInstant(ZoneOffset.UTC).toEpochMilli();
    }
  }
}
