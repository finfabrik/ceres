package com.blokaly.ceres.bitfinex.event;

import com.blokaly.ceres.common.DecimalNumber;
import com.blokaly.ceres.data.IdBasedOrderInfo;
import com.blokaly.ceres.data.MarketDataIncremental;
import com.blokaly.ceres.data.OrderInfo;
import com.google.gson.JsonArray;

import java.util.Collection;
import java.util.Collections;

import static com.blokaly.ceres.data.MarketDataIncremental.Type.DONE;
import static com.blokaly.ceres.data.MarketDataIncremental.Type.UPDATE;

public class RefreshEvent extends ChannelEvent implements MarketDataIncremental<IdBasedOrderInfo> {

    private final JsonArray data;
    private final long sequence;
    private final Type type;
    private final IdBasedOrderInfo orderInfo;

    private static Type parseType(JsonArray data) {
        if (parseDecimal(data, 1).isZero()) {
            return DONE;
        } else {
            return UPDATE;
        }
    }

    public RefreshEvent(int channelId, long sequence, JsonArray data) {
        super(channelId, "refresh");
        this.data = data;
        this.sequence = sequence;
        this.type = parseType(data);
        this.orderInfo = initOrderInfo();
    }

    private IdBasedOrderInfo initOrderInfo() {
        if (type == UPDATE) {
            return new UpdateOrderInfo();
        } else {
            return new DoneOrderInfo();
        }
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public long getSequence() {
        return sequence;
    }

    @Override
    public Collection<IdBasedOrderInfo> orderInfos() {
        return Collections.singletonList(orderInfo);
    }

    @Override
    public String toString() {
        return "RefreshEvent(" + sequence + ")" + orderInfo;
    }

    private static DecimalNumber parseDecimal(JsonArray data, int field) {
        return DecimalNumber.fromDbl(data.get(field).getAsDouble());
    }

    private class MDIncrementalOrderInfo implements IdBasedOrderInfo {

        private final DecimalNumber quantity;
        private final Side side;

        public MDIncrementalOrderInfo() {
            DecimalNumber qty = parseDecimal(data, 2);
            side = qty.compareTo(DecimalNumber.ZERO) > 0 ?  OrderInfo.Side.BUY : OrderInfo.Side.SELL;
            quantity = qty.abs();
        }

        @Override
        public DecimalNumber getPrice() {
            return parseDecimal(data, 1);
        }

        @Override
        public DecimalNumber getQuantity() {
            return quantity;
        }

        @Override
        public String getId() {
            return data.get(0).getAsString();
        }

        @Override
        public Side side() {
            return side;
        }
    }

    private class UpdateOrderInfo extends MDIncrementalOrderInfo {
        public String toString() {
            return "[U," + getPrice() + "," + getQuantity() + "]";
        }
    }

    private class DoneOrderInfo extends MDIncrementalOrderInfo {
        public String toString() {
            return "[D," + getId() + "]";
        }
    }
}
