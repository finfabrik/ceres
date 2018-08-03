package com.blokaly.ceres.bitfinex.callback;

import com.blokaly.ceres.bitfinex.event.*;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

import java.util.concurrent.ConcurrentMap;

@Singleton
public class ChannelCallbackHandler implements CommandCallbackHandler<ChannelEvent>{

  private static long sequence = 0;
  private final ConcurrentMap<Integer, SubscriptionEvent> channelMap;

  @Inject
  public ChannelCallbackHandler(@Named("ChannelMap") ConcurrentMap<Integer, SubscriptionEvent> channelMap) {
    this.channelMap = channelMap;
  }

  @Override
  public ChannelEvent handleEvent(JsonElement json, JsonDeserializationContext context) {
    JsonArray chanArray = json.getAsJsonArray();
    int channelId = chanArray.get(0).getAsInt();
    SubscriptionEvent sub = channelMap.get(channelId);
    if (sub == null || !sub.getChannel().equalsIgnoreCase(ChannelEvent.ORDERBOOK_CHANNEL)) {
      return ChannelEvent.UNKNOWN_EVENT;
    }

    JsonElement element = chanArray.get(1);
    if (element.isJsonArray()) {
      return OrderBookSnapshot.parse(channelId, ++sequence, element.getAsJsonArray());
    } else {
      String stringData = element.getAsString();
      if ("hb".equals(stringData)) {
        return new HbEvent(channelId);
      } else {
        return new OrderBookRefresh(channelId, ++sequence, chanArray);
      }
    }
  }
}
