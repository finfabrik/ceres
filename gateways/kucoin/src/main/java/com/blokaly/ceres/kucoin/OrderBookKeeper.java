package com.blokaly.ceres.kucoin;

import com.blokaly.ceres.orderbook.DepthBasedOrderBook;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.Map;

@Singleton
public class OrderBookKeeper {
    private final Map<String, DepthBasedOrderBook> orderBooks;

    @Inject
    public OrderBookKeeper(Map<String, DepthBasedOrderBook> orderBooks){
        this.orderBooks = orderBooks;
    }

    public DepthBasedOrderBook get(String symbol) {
        return orderBooks.get(symbol);
    }

    public Collection<String> getAllSymbols(){
        return orderBooks.keySet();
    }

    public Collection<DepthBasedOrderBook> getAllBooks(){
        return orderBooks.values();
    }
}

