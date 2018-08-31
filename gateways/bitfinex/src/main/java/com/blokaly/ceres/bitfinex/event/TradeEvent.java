package com.blokaly.ceres.bitfinex.event;

import com.blokaly.ceres.common.DecimalNumber;
import com.blokaly.ceres.data.OrderInfo;
import com.google.gson.JsonArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TradeEvent extends ChannelEvent {
  private static final Logger LOGGER = LoggerFactory.getLogger(TradeEvent.class);
  private final Collection<Trade> trades;

  private TradeEvent(int channel) {
   super(channel, "trade");
    trades = Collections.emptyList();
  }

  public TradeEvent(int channelId, List<Trade> trades) {
    super(channelId, "trade");
    this.trades = trades;
  }

  public Collection<Trade> getTrades() {
    return trades;
  }

  public static TradeEvent parse(int channelId, JsonArray data) {
    if (data == null || data.size()==0) {
      return new TradeEvent(channelId);
    }

    List<Trade> trades = Collections.emptyList();
    try {
      if (data.get(0).isJsonArray()) {
        trades = StreamSupport.stream(data.spliterator(), false)
            .map(elm -> Trade.parse(elm.getAsJsonArray()))
            .collect(Collectors.toList());
      } else {
        trades = Collections.singletonList(Trade.parse(data));
      }
    } catch (Exception ex) {
      LOGGER.error("Error parsing message: " + data, ex);
    }

    return new TradeEvent(channelId, trades);
  }

  public static class Trade {

    private final long time;
    private final DecimalNumber price;
    private final DecimalNumber quantity;
    private final OrderInfo.Side side;
    private final long tradeId;

    private Trade(long time, DecimalNumber price, DecimalNumber quantity, OrderInfo.Side side, long tradeId) {
      this.time = time;
      this.price = price;
      this.quantity = quantity;
      this.side = side;
      this.tradeId = tradeId;
    }

    public long getTime() {
      return time;
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

    public long getTradeId() {
      return tradeId;
    }

    private static Trade parse(JsonArray trade) {
      if (trade.size() == 4) {
        long time = trade.get(1).getAsLong();
        DecimalNumber price = DecimalNumber.fromStr(trade.get(2).getAsString());
        DecimalNumber size = DecimalNumber.fromStr(trade.get(3).getAsString());
        OrderInfo.Side side = size.compareTo(DecimalNumber.ZERO) >0 ? OrderInfo.Side.BUY : OrderInfo.Side.SELL;
        return new Trade(time, price, size.abs(), side, 0L);
      } else if (trade.size() == 6) {
        long time = trade.get(3).getAsLong();
        DecimalNumber price = DecimalNumber.fromStr(trade.get(4).getAsString());
        DecimalNumber size = DecimalNumber.fromStr(trade.get(5).getAsString());
        OrderInfo.Side side = size.compareTo(DecimalNumber.ZERO) >0 ? OrderInfo.Side.BUY : OrderInfo.Side.SELL;
        return new Trade(time, price, size.abs(), side, 0L);
      } else if (trade.size() == 7) {
        long tradeId = trade.get(3).getAsLong();
        long time = trade.get(4).getAsLong();
        DecimalNumber price = DecimalNumber.fromStr(trade.get(5).getAsString());
        DecimalNumber size = DecimalNumber.fromStr(trade.get(6).getAsString());
        OrderInfo.Side side = size.compareTo(DecimalNumber.ZERO) >0 ? OrderInfo.Side.BUY : OrderInfo.Side.SELL;
        return new Trade(time, price, size.abs(), side, tradeId);
      } else {
        throw new IllegalArgumentException("not a trade message: " + trade);
      }
    }
  }
}
