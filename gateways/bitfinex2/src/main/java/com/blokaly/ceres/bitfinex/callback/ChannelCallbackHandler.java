package com.blokaly.ceres.bitfinex.callback;

import com.blokaly.ceres.bitfinex.event.ChannelEvent;
import com.blokaly.ceres.bitfinex.event.HbEvent;
import com.blokaly.ceres.bitfinex.event.RefreshEvent;
import com.blokaly.ceres.bitfinex.event.SnapshotEvent;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelCallbackHandler implements CommandCallbackHandler<ChannelEvent>{
    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelCallbackHandler.class);
    @Override
    public ChannelEvent handleEvent(JsonElement json, JsonDeserializationContext context) {
        JsonArray data = json.getAsJsonArray();
        int channelId = data.get(0).getAsInt();
        JsonElement elm = data.get(1);
        if (elm.isJsonArray()) {
            JsonArray orderbook = elm.getAsJsonArray();
            long sequence = data.get(2).getAsLong();
            if (orderbook.get(0).isJsonArray()) {
                return SnapshotEvent.parse(channelId, sequence, orderbook);
            } else {
                return new RefreshEvent(channelId, sequence, orderbook);
            }

        } else {
            try {
                if ("hb".equals(elm.getAsString())) {
                    return new HbEvent(channelId);
                }
            } catch (Exception ex) {
               LOGGER.error("Unknown channel event");
            }
        }
        return null;
    }
}
