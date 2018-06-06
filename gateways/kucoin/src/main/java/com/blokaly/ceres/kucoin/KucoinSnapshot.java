package com.blokaly.ceres.kucoin;

import com.blokaly.ceres.common.DecimalNumber;
import com.blokaly.ceres.data.DepthBasedOrderInfo;
import com.blokaly.ceres.data.MarketDataSnapshot;
import com.blokaly.ceres.data.OrderInfo;
import com.blokaly.ceres.orderbook.DepthBasedOrderBook;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Decimal;
import org.joda.time.DateTimeUtils;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.kucoin.dto.marketdata.KucoinOrderBook;
import org.knowm.xchange.kucoin.dto.KucoinResponse;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

public class KucoinSnapshot implements MarketDataSnapshot<DepthBasedOrderInfo> {

    private final long sequence;
    private final Collection<DepthBasedOrderInfo> bids;
    private final Collection<DepthBasedOrderInfo> asks;

    private KucoinSnapshot(long sequence, Collection<DepthBasedOrderInfo> bids, Collection<DepthBasedOrderInfo> asks){
        this.sequence = sequence;
        this.bids = bids;
        this.asks = asks;
    }

    public static KucoinSnapshot parse(KucoinOrderBook orderBook){
        ArrayList<DepthBasedOrderInfo> bids = new ArrayList<>(getOrderInfoList(orderBook.getBuy(), OrderInfo.Side.BUY));
        ArrayList<DepthBasedOrderInfo> asks = new ArrayList<>(getOrderInfoList(orderBook.getSell(), OrderInfo.Side.SELL));;
        return new KucoinSnapshot(DateTimeUtils.currentTimeMillis(), bids, asks);
    }

    private static ArrayList<DepthBasedOrderInfo> getOrderInfoList(List<List<BigDecimal>> bids, OrderInfo.Side side){
        ArrayList<DepthBasedOrderInfo> infos = Lists.newArrayListWithCapacity(bids.size());
        int idx = 0;
        for(List<BigDecimal> order : bids){
            infos.add(new KucionPublicOrderWrapper(idx++, side, DecimalNumber.fromBD(order.get(0)), DecimalNumber.fromBD(order.get(1))));
        }
        return infos;
    }

    @Override
    public long getSequence() {
        return sequence;
    }

    @Override
    public Collection<DepthBasedOrderInfo> getBids() {
        return bids;
    }

    @Override
    public Collection<DepthBasedOrderInfo> getAsks() {
        return asks;
    }

    private static class KucionPublicOrderWrapper implements DepthBasedOrderInfo{
        private final int depth;
        private final Side side;
        private final DecimalNumber price;
        private final DecimalNumber quantity;

        private KucionPublicOrderWrapper(int depth, Side side, DecimalNumber price, DecimalNumber quantity){
            this.depth = depth;
            this.side = side;
            this.price = price;
            this.quantity = quantity;
        }

        @Override
        public int getDepth() {
            return depth;
        }

        @Override
        public Side side() {
            return null;
        }

        @Override
        public DecimalNumber getPrice() {
            return price;
        }

        @Override
        public DecimalNumber getQuantity() {
            return quantity;
        }

    }

}

