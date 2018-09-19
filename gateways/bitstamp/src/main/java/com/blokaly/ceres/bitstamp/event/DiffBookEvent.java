package com.blokaly.ceres.bitstamp.event;

import com.blokaly.ceres.data.MarketDataIncremental;
import com.blokaly.ceres.data.OrderInfo;
import com.blokaly.ceres.utils.StreamEvent;
import com.google.gson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DiffBookEvent implements StreamEvent {

    private final long sequence;
    private final MarketDataIncremental<OrderInfo> update;
    private final MarketDataIncremental<OrderInfo> deletion;

    public static DiffBookEvent parse(long sequence, JsonArray bidArray, JsonArray askArray) {

        Map<Boolean, List<OrderInfo>> bids = StreamSupport.stream(bidArray.spliterator(), false)
                .map(elm -> new JsonArrayOrderInfo(OrderInfo.Side.BUY, elm.getAsJsonArray()))
                .collect(Collectors.partitioningBy(order -> order.getQuantity().isZero()));

        Map<Boolean, List<OrderInfo>> asks = StreamSupport.stream(askArray.spliterator(), false)
                .map(elm -> new JsonArrayOrderInfo(OrderInfo.Side.SELL, elm.getAsJsonArray()))
                .collect(Collectors.partitioningBy(order -> order.getQuantity().isZero()));

        RefreshEvent update = new RefreshEvent(sequence, MarketDataIncremental.Type.UPDATE);
        update.add(bids.get(false));
        update.add(asks.get(false));

        RefreshEvent deletion = new RefreshEvent(sequence, MarketDataIncremental.Type.DONE);
        deletion.add(bids.get(true));
        deletion.add(asks.get(true));

        return new DiffBookEvent(sequence, update, deletion);
    }

    public DiffBookEvent(long sequence, MarketDataIncremental<OrderInfo> update, MarketDataIncremental<OrderInfo> deletion) {
        this.sequence = sequence;
        this.update = update;
        this.deletion = deletion;
    }

    public long getSequence() {
        return sequence;
    }

    public MarketDataIncremental<OrderInfo> getUpdate() {
        return update;
    }

    public MarketDataIncremental<OrderInfo> getDeletion() {
        return deletion;
    }

    public static class DiffBookEventAdapter implements JsonDeserializer<DiffBookEvent> {

        private static final Logger LOGGER = LoggerFactory.getLogger(DiffBookEventAdapter.class);

        @Override
        public DiffBookEvent deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

            JsonObject jsonObject = json.getAsJsonObject();
            long sequence = jsonObject.get("timestamp").getAsLong();
            JsonArray bids = jsonObject.get("bids").getAsJsonArray();
            JsonArray asks = jsonObject.get("asks").getAsJsonArray();
            return parse(sequence, bids, asks);
        }
    }
}
