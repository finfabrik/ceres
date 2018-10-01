package com.blokaly.ceres.hitbtc.data;

import com.blokaly.ceres.common.DecimalNumber;
import com.blokaly.ceres.data.MarketDataSnapshot;
import com.blokaly.ceres.data.OrderInfo;
import com.blokaly.ceres.hitbtc.event.AbstractEvent;
import com.blokaly.ceres.hitbtc.event.EventType;
import com.google.gson.JsonArray;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class OrderbookSnapshot extends AbstractEvent implements MarketDataSnapshot {

    private String symbol;
    private long sequence;
    private List<OrderInfo> ask;
    private List<OrderInfo> bid;

    @Override
    public long getSequence() {
        return sequence;
    }

    @Override
    public Collection getAsks() { return ask; }

    @Override
    public Collection getBids() {
        return bid;
    }

    public String getSymbol() {
        return symbol;
    }

    private OrderbookSnapshot(String symbol, long sequence, List<OrderInfo> ask, List<OrderInfo> bid){
        super(EventType.ORDERBOOK_SNAPSHOT.getType());
        this.symbol = symbol;
        this.sequence = sequence;
        this.ask = ask;
        this.bid = bid;
    }

    public static OrderbookSnapshot parse(String symbol, long sequence, JsonArray asksArray, JsonArray bidsArray ){
        List<OrderInfo> ask = StreamSupport.stream(asksArray.spliterator(), false).map(elm -> new OrderRecord(DecimalNumber.fromBD(elm.getAsJsonObject().get("price").getAsBigDecimal()), DecimalNumber.fromBD(elm.getAsJsonObject().get("size").getAsBigDecimal()), OrderInfo.Side.SELL)).collect(Collectors.toList());
        List<OrderInfo> bid = StreamSupport.stream(bidsArray.spliterator(), false).map(elm -> new OrderRecord(DecimalNumber.fromBD(elm.getAsJsonObject().get("price").getAsBigDecimal()), DecimalNumber.fromBD(elm.getAsJsonObject().get("size").getAsBigDecimal()), OrderInfo.Side.BUY)).collect(Collectors.toList());
        return new OrderbookSnapshot(symbol, sequence, ask, bid);
    }

}
