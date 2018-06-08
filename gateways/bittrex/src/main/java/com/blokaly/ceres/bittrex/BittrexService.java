package com.blokaly.ceres.bittrex;

import com.blokaly.ceres.binding.BootstrapService;
import com.blokaly.ceres.binding.CeresModule;
import com.blokaly.ceres.bittrex.event.ExchangeDeltaEvent;
import com.blokaly.ceres.bittrex.event.ExchangeStateEvent;
import com.blokaly.ceres.common.*;
import com.blokaly.ceres.data.SymbolFormatter;
import com.blokaly.ceres.kafka.HBProducer;
import com.blokaly.ceres.kafka.KafkaCommonModule;
import com.blokaly.ceres.kafka.KafkaStreamModule;
import com.blokaly.ceres.kafka.ToBProducer;
import com.blokaly.ceres.orderbook.PriceAggregatedOrderBook;
import com.blokaly.ceres.system.CommonConfigs;
import com.blokaly.ceres.system.Services;
import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.web.HandlerModule;
import com.blokaly.ceres.web.UndertowModule;
import com.blokaly.ceres.web.handlers.HealthCheckHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.*;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import io.undertow.Undertow;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class BittrexService extends BootstrapService {
  private final BittrexClient client;
  private final KafkaStreams streams;
  private final Undertow undertow;

  @Inject
  public BittrexService(BittrexClient client,
                        @Named("Throttled") KafkaStreams streams,
                        Undertow undertow) {
    this.client = client;
    this.streams = streams;
    this.undertow = undertow;
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("starting Bittrex client...");
    client.start();

    waitFor(3);
    LOGGER.info("starting kafka streams...");
    streams.start();

    LOGGER.info("Web server starting...");
    undertow.start();
  }

  @Override
  protected void shutDown() throws Exception {
    LOGGER.info("Web server stopping...");
    undertow.stop();

    LOGGER.info("stopping Bittrex client...");
    client.stop();

    LOGGER.info("stopping kafka streams...");
    streams.close();
  }

  public static class BitstampModule extends CeresModule {

    @Override
    protected void configure() {
      this.install(new UndertowModule(new HandlerModule() {

        @Override
        protected void configureHandlers() {
          this.bindHandler().to(HealthCheckHandler.class);
        }
      }));
      expose(Undertow.class);

      install(new KafkaCommonModule());
      install(new KafkaStreamModule());
      bindExpose(ToBProducer.class);
      bind(HBProducer.class).asEagerSingleton();

      expose(StreamsBuilder.class).annotatedWith(Names.named("Throttled"));
      expose(KafkaStreams.class).annotatedWith(Names.named("Throttled"));

    }

    @Provides
    @Singleton
    @Exposed
    public BittrexClient provideBittrexClient(Config config, OrderBookKeeper keeper, Gson gson) {
      return new BittrexClient(config, keeper, gson);
    }

    @Provides
    @Singleton
    @Exposed
    public OrderBookKeeper provideOrderBookKeeper(Config config, Provider<BittrexClient> clientProvider, Gson gson, ToBProducer producer, @SingleThread Provider<ExecutorService> provider) {
      String source = Source.valueOf(config.getString(CommonConfigs.APP_SOURCE).toUpperCase()).getCode();
      return new OrderBookKeeper(
          config.getStringList("symbols").stream().map(sym -> {
            PairSymbol market = parseSymbol(sym);
            String code = market.getBase().equalsIgnoreCase("usdt") ? market.invert().getCode() : market.getCode();
            return new OrderBookHandler(market, clientProvider, new PriceAggregatedOrderBook(code, code + "." + source), producer, gson, provider.get());
          }).collect(Collectors.toMap(OrderBookHandler::getMarket, handler -> handler))
      );
    }

    private PairSymbol parseSymbol(String symbol) {
      String[] syms = symbol.split("-");
      return SymbolFormatter.normalise(syms[0], syms[1]);
    }

    @Provides
    @Singleton
    public Gson provideGson() {
      GsonBuilder builder = new GsonBuilder();
      builder.registerTypeAdapter(ExchangeStateEvent.class, new ExchangeStateEventAdapter());
      builder.registerTypeAdapter(ExchangeDeltaEvent.class, new ExchangeDeltaEventAdapter());
      return builder.create();
    }
  }

  public static void main(String[] args) {
    Services.start(new BitstampModule());
  }
}
