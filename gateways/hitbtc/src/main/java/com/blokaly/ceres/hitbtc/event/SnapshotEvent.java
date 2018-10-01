package com.blokaly.ceres.hitbtc.event;

import com.blokaly.ceres.hitbtc.data.OrderbookSnapshot;
import com.blokaly.ceres.hitbtc.data.OrderRecord;

import java.util.List;

public class SnapshotEvent extends ChannelEvent {

    SnapshotEvent(String method, OrderbookSnapshot orderBookSnapshot){
        super(method, orderBookSnapshot);
    }

    @Override
    public String toString(){
        return "SnapshotEvent{" +
                "method=" + getMethod() +
                ", sequence=" + orderBookSnapshot.getSequence() +
                ", symbol=" + orderBookSnapshot.getSymbol() +
                ", asks=" + (List<OrderRecord>) orderBookSnapshot.getAsks() +
                ", bids=" + (List<OrderRecord>) orderBookSnapshot.getBids() +
                "}";
    }


}
