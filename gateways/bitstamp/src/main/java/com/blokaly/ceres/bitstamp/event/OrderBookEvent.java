package com.blokaly.ceres.bitstamp.event;

import com.blokaly.ceres.data.MarketDataSnapshot;
import com.blokaly.ceres.data.OrderInfo;
import com.google.gson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class OrderBookEvent implements MarketDataSnapshot<OrderInfo> {
    private final long sequence;
    private final Collection<OrderInfo> bids;
    private final Collection<OrderInfo> asks;

    public static OrderBookEvent parse(long sequence, JsonArray bidArray, JsonArray askArray) {

        List<OrderInfo> bids = StreamSupport.stream(bidArray.spliterator(), false).map(elm -> new JsonArrayOrderInfo(OrderInfo.Side.BUY, elm.getAsJsonArray())).collect(Collectors.toList());
        List<OrderInfo> asks = StreamSupport.stream(askArray.spliterator(), false).map(elm -> new JsonArrayOrderInfo(OrderInfo.Side.SELL, elm.getAsJsonArray())).collect(Collectors.toList());
        return new OrderBookEvent(sequence, bids, asks);
    }

    private OrderBookEvent(long sequence, Collection<OrderInfo> bids, Collection<OrderInfo> asks) {
        this.sequence = sequence;
        this.bids = bids;
        this.asks = asks;
    }

    @Override
    public long getSequence() {
        return sequence;
    }

    @Override
    public Collection<OrderInfo> getBids() {
        return bids;
    }

    @Override
    public Collection<OrderInfo> getAsks() {
        return asks;
    }

    @Override
    public String toString() {
        return "OrderBookEvent{" +
                "sequence=" + sequence +
                ", bids=" + bids +
                ", asks=" + asks +
                '}';
    }

  public static class OrderBookEventAdapter implements JsonDeserializer<OrderBookEvent> {

      private static final Logger LOGGER = LoggerFactory.getLogger(OrderBookEventAdapter.class);

      @Override
      public OrderBookEvent deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

          JsonObject jsonObject = json.getAsJsonObject();
          long sequence = jsonObject.get("timestamp").getAsLong();
          JsonArray bids = jsonObject.get("bids").getAsJsonArray();
          JsonArray asks = jsonObject.get("asks").getAsJsonArray();
          return parse(sequence, bids, asks);
      }
  }
}
