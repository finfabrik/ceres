package com.blokaly.ceres.hitbtc.data;

import com.blokaly.ceres.data.MarketDataIncremental;
import com.blokaly.ceres.data.OrderInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MarketDataIncrementalTagged implements MarketDataIncremental<OrderInfo>{

    private final long sequence;
    private final Type type;
    private final List<OrderInfo> orderInfoList;

    public MarketDataIncrementalTagged(long sequence, Type type) {
        this.sequence = sequence;
        this.type = type;
        orderInfoList = new ArrayList<>();
    }

    @Override
    public long getSequence() {
        return sequence;
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public Collection orderInfos() {
        return orderInfoList;
    }

    public void add(List<OrderInfo> orders) {
        orderInfoList.addAll(orders);
    }

    @Override
    public String toString() {
        return "MarketDataIncrementalTagged{" +
                "sequence=" + sequence +
                ", type=" + type +
                ", orderinfo=" + orderInfoList +
                '}';
    }
}
