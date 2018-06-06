package com.blokaly.ceres.kucoin;

import com.blokaly.ceres.binding.BootstrapService;
import com.blokaly.ceres.binding.CeresModule;
import com.blokaly.ceres.common.CommonConfigs;
import com.blokaly.ceres.common.Services;
import com.blokaly.ceres.common.Source;
import com.blokaly.ceres.data.SymbolFormatter;
import com.blokaly.ceres.kafka.HBProducer;
import com.blokaly.ceres.kafka.KafkaCommonModule;
import com.blokaly.ceres.kafka.KafkaStreamModule;
import com.blokaly.ceres.kafka.ToBProducer;
import com.blokaly.ceres.orderbook.DepthBasedOrderBook;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Exposed;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.kucoin.KucoinExchange;
import org.knowm.xchange.kucoin.service.KucoinMarketDataServiceRaw;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class KucoinService extends BootstrapService{
    private final MarketDataHandler handler;
    private final KafkaStreams streams;

    @Inject
    public KucoinService(MarketDataHandler handler, @Named("Throttled") KafkaStreams streams){
        this.handler = handler;
        this.streams = streams;
    }

    @Override
    protected void startUp() throws Exception{
        LOGGER.info("starting kucoin market data handler...");
        handler.start();

        waitFor(3);
        LOGGER.info("starting kafka streams...");
        streams.start();
    }

    @Override
    protected void shutDown() throws Exception {
        LOGGER.info("stopping kucoin market data handler...");
        handler.stop();
        LOGGER.info("stopping kafka streams...");
        streams.close();
    }

    public static class KucoinModule extends CeresModule{
        @Override
        protected void configure(){
            install(new KafkaCommonModule());
            install(new KafkaStreamModule());
            bindExpose(ToBProducer.class);
            bind(HBProducer.class).asEagerSingleton();
            expose(StreamsBuilder.class).annotatedWith(Names.named("Throttled"));
            expose(KafkaStreams.class).annotatedWith(Names.named("Throttled"));

            bind(org.knowm.xchange.Exchange.class).toInstance(ExchangeFactory.INSTANCE.createExchange(KucoinExchange.class.getName()));
        }

        @Provides
        @Singleton
        @Exposed
        public Gson provideGson(){
            GsonBuilder builder = new GsonBuilder();
            return builder.create();
        }

        @Exposed
        @Provides
        @Singleton
        public  KucoinMarketDataServiceRaw provideMarketDataService(org.knowm.xchange.Exchange exchange){
            return (KucoinMarketDataServiceRaw) exchange.getMarketDataService();
        }

        @Exposed
        @Provides
        @Singleton
        public Map<String, DepthBasedOrderBook> provideOrderBooks(Config config){
            List<String> symbols = config.getStringList("symbols");
            int depth = config.getInt("depth");
            String source = Source.valueOf(config.getString(CommonConfigs.APP_SOURCE).toUpperCase()).getCode();
            //String source = "";
            return symbols.stream().collect(Collectors.toMap(sym->sym, sym -> {
                String symbol = SymbolFormatter.normalise(sym);
                return new DepthBasedOrderBook(sym, depth, symbol + "." + source);
            }));
        }

    }
    public static void main(String[] args){
        Services.start(new KucoinModule());
    }
}
