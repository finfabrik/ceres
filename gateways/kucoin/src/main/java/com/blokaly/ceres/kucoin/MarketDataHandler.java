package com.blokaly.ceres.kucoin;

import com.blokaly.ceres.common.SingleThread;
import com.blokaly.ceres.kafka.ToBProducer;
import com.blokaly.ceres.orderbook.DepthBasedOrderBook;
import com.typesafe.config.Config;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.kucoin.dto.KucoinResponse;
import org.knowm.xchange.kucoin.dto.marketdata.KucoinOrderBook;
import org.knowm.xchange.kucoin.service.KucoinMarketDataServiceRaw;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.Time;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Singleton
public class MarketDataHandler {
    private static Logger LOGGER = LoggerFactory.getLogger(MarketDataHandler.class);
    private final KucoinMarketDataServiceRaw service;
    private final ScheduledExecutorService ses;
    private final OrderBookKeeper keeper;
    private final ToBProducer producer;
    private final int depth;

    @Inject
    public MarketDataHandler(KucoinMarketDataServiceRaw service,
                             @SingleThread ScheduledExecutorService ses,
                             OrderBookKeeper keeper,
                             ToBProducer producer,
                             Config config){
        this.service = service;
        this.ses = ses;
        this.keeper = keeper;
        this.producer = producer;
        depth = config.getInt("depth");
    }

    public void start(){
        int idx =0;
        for(String sym : keeper.getAllSymbols()){
            ses.schedule(() -> {pullMarketData(sym);}, ++idx * 3, TimeUnit.SECONDS);
        }
    }

    //#tentative
    //Kind of uncool : two variable orderBook and orderbook
    //Keeping it consistent for now
    private void pullMarketData(String sym){
        try{
            KucoinResponse<KucoinOrderBook> kucoinOrderBook = service.getKucoinOrderBook(new CurrencyPair(sym), depth);
            LOGGER.debug("{}", kucoinOrderBook);
            KucoinSnapshot snapshot = KucoinSnapshot.parse(kucoinOrderBook.getData());
            DepthBasedOrderBook orderBook = keeper.get(sym);
            orderBook.processSnapshot(snapshot);
            producer.publish(orderBook);
        }catch (Exception e){
            LOGGER.error("Failed to retrieve depth", e);
        }finally{
            ses.schedule(() -> {pullMarketData(sym);}, 3, TimeUnit.SECONDS);
        }
    }

    public void stop(){}

}
