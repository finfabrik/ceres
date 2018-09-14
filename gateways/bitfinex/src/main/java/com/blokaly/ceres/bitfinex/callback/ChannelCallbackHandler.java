package com.blokaly.ceres.bitfinex.callback;

import com.blokaly.ceres.bitfinex.event.*;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;

@Singleton
public class ChannelCallbackHandler implements CommandCallbackHandler<ChannelEvent>{
  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelCallbackHandler.class);
  private final ConcurrentMap<Integer, SubscriptionEvent> channelMap;

  @Inject
  public ChannelCallbackHandler(@Named("ChannelMap") ConcurrentMap<Integer, SubscriptionEvent> channelMap) {
    this.channelMap = channelMap;
  }

  @Override
  public ChannelEvent handleEvent(JsonElement json, JsonDeserializationContext context) {
    long now = System.currentTimeMillis();
    JsonArray jsonArray = json.getAsJsonArray();
    int channelId = jsonArray.get(0).getAsInt();
    SubscriptionEvent sub = channelMap.get(channelId);
    if (sub == null) {
      return ChannelEvent.UNKNOWN_EVENT;
    }

    switch (sub.getChannel()) {
      case ChannelEvent.ORDERBOOK_CHANNEL:
      {
        return parseOrderBookEvent(now, jsonArray, channelId);
      }
      case ChannelEvent.TRADE_CHANNEL:{
        return parseTradeEvent(now, jsonArray, channelId);
      }
      default: return ChannelEvent.UNKNOWN_EVENT;
    }


  }

  private ChannelEvent parseOrderBookEvent(long now, JsonArray jsonArray, int channelId) {
    JsonElement element = jsonArray.get(1);
    if (element.isJsonArray()) {
      return OrderBookSnapshot.parse(channelId, now, element.getAsJsonArray());
    } else {
      String stringData = element.getAsString();
      if ("hb".equals(stringData)) {
        return new HbEvent(channelId);
      } else {
        return new OrderBookRefresh(channelId, now, jsonArray);
      }
    }
  }

  private ChannelEvent parseTradeEvent(long now, JsonArray jsonArray, int channelId) {
    JsonElement element = jsonArray.get(1);
    if (element.isJsonArray()) {
      return TradeEvent.parse(now, channelId, element.getAsJsonArray());
    } else {
      String stringData = element.getAsString();
      if ("hb".equals(stringData)) {
        return new HbEvent(channelId);
      } else {
        return TradeEvent.parse(now, channelId, jsonArray);
      }
    }
  }
}
