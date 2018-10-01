package com.blokaly.ceres.hitbtc;

import com.blokaly.ceres.binding.AwaitExecutionService;
import com.blokaly.ceres.binding.BootstrapService;
import com.blokaly.ceres.binding.CeresModule;
import com.blokaly.ceres.common.Configs;
import com.blokaly.ceres.common.PairSymbol;
import com.blokaly.ceres.common.Source;
import com.blokaly.ceres.data.SymbolFormatter;
import com.blokaly.ceres.hitbtc.callback.*;
import com.blokaly.ceres.hitbtc.event.AbstractEvent;
import com.blokaly.ceres.hitbtc.event.EventType;
import com.blokaly.ceres.influxdb.ringbuffer.BatchedPointsPublisher;
import com.blokaly.ceres.influxdb.ringbuffer.InfluxdbBufferModule;
import com.blokaly.ceres.kafka.HBProducer;
import com.blokaly.ceres.kafka.KafkaCommonModule;
import com.blokaly.ceres.kafka.KafkaStreamModule;
import com.blokaly.ceres.kafka.ToBProducer;
import com.blokaly.ceres.orderbook.PriceBasedOrderBook;
import com.blokaly.ceres.orderbook.TopOfBookProcessor;
import com.blokaly.ceres.system.CommonConfigs;
import com.blokaly.ceres.system.Services;
import com.blokaly.ceres.web.HandlerModule;
import com.blokaly.ceres.web.UndertowModule;
import com.blokaly.ceres.web.handlers.HealthCheckHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Exposed;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import io.undertow.Undertow;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.blokaly.ceres.hitbtc.event.EventType.*;

public class HitbtcService {

  private static boolean kafkaEnabled;
  private static boolean undertowEnabled;
  static {
    Config config = Configs.convertSystemUnderscoreToDot();
    kafkaEnabled = Configs.getOrDefault(config, "kafka.enabled", Configs.BOOLEAN_EXTRACTOR, false);
    undertowEnabled = Configs.getOrDefault(config, "undertow.enabled", Configs.BOOLEAN_EXTRACTOR, false);
  }

  public static class Client extends AwaitExecutionService {
    private final HitbtcClientProvider provider;

    @Inject
    public Client(HitbtcClientProvider provider) {
      this.provider = provider;
    }

    @Override
    protected void startUp() throws Exception {
      LOGGER.info("starting websocket client...");
      provider.start();
    }

    @Override
      protected void shutDown() throws Exception {
      LOGGER.info("stopping websocket client...");
      provider.stop();
    }
  }

  public static class Kafka extends BootstrapService {
    private final KafkaStreams streams;

    @Inject
    public Kafka(@Named("Throttled") KafkaStreams streams) {
      this.streams = streams;
    }

    public static boolean isEnabled() {
      return kafkaEnabled;
    }

    @Override
    protected void startUp() throws Exception {
      waitFor(3);
      LOGGER.info("starting kafka streams...");
      streams.start();

    }

    @Override
    protected void shutDown() throws Exception {
      LOGGER.info("stopping kafka streams...");
      streams.close();
    }
  }

  public static class Health extends BootstrapService {
    private final Undertow undertow;

    @Inject
    public Health(Undertow server) {
      this.undertow = server;
    }

    public static boolean isEnabled() {
      return undertowEnabled;
    }

    @Override
    protected void startUp() throws Exception {
      LOGGER.info("Web server starting...");
      undertow.start();
    }

    @Override
    protected void shutDown() throws Exception {
      LOGGER.info("Web server stopping...");
      undertow.stop();
    }
  }

  public static void main(String args[]) {
    Services.start(new HitbtcModule());
  }


  public static class HitbtcModule extends CeresModule {

    @Override
    protected void configure() {
      configUndertow();
      configKafka();

      bindAllCallbacks();
      bindExpose(MessageHandler.class).to(MessageHandlerImpl.class).in(Singleton.class);
      bindExpose(HitbtcClient.class).toProvider(HitbtcClientProvider.class).in(Singleton.class);

      install(new InfluxdbBufferModule());
      bindExpose(BatchedPointsPublisher.class);

     }

    @Provides
    @Exposed
    public URI provideUri(Config config) throws Exception {
      return new URI(config.getString(CommonConfigs.WS_URL));
    }

    @Provides
    @Exposed
    @Singleton
    public Gson provideGson(Map<EventType, CommandCallbackHandler> handlers) {
      GsonBuilder builder = new GsonBuilder();
      builder.registerTypeAdapter(AbstractEvent.class, new EventAdapter(handlers));
      return builder.create();
    }

    private void bindAllCallbacks() {
      MapBinder<EventType, CommandCallbackHandler> binder = MapBinder.newMapBinder(binder(), EventType.class, CommandCallbackHandler.class);
      binder.addBinding(SUBSCRIPTION).to(SubscribedCallbackHandler.class);
      binder.addBinding(ERROR).to(ErrorCallbackHandler.class);
      binder.addBinding(ORDERBOOK_SNAPSHOT).to(OrderBookSnapshotCallbackHandler.class);
      binder.addBinding(ORDERBOOK_UPDATE).to(OrderBookUpdateCallbackHandler.class);
      binder.addBinding(TRADES_SNAPSHOT).to(TradesSnapshotCallbackHandler.class);
      binder.addBinding(TRADES_UPDATE).to(TradesUpdateCallbackHandler.class);
    }

    @Provides
    @Singleton
    @Exposed
    @Named("SymbolMap")
    public Map<String, PairSymbol> provideSymbolMap(Config config) {
      return config.getStringList("symbols").stream().collect(Collectors.toMap(SymbolFormatter::normalise, PairSymbol::parse));
    }

    private void configKafka() {
      if (kafkaEnabled) {
        install(new KafkaCommonModule());
        install(new KafkaStreamModule());
        bindExpose(ToBProducer.class);
        bind(HBProducer.class).asEagerSingleton();
        expose(StreamsBuilder.class).annotatedWith(Names.named("Throttled"));
        expose(KafkaStreams.class).annotatedWith(Names.named("Throttled"));
      } else {
        bind(TopOfBookProcessor.class).to(TopOfBookProcessor.NoOpProcessor.class);
      }
    }

    private void configUndertow() {
      if (undertowEnabled) {
        this.install(new UndertowModule(new HandlerModule() {

          @Override
          protected void configureHandlers() {
            this.bindHandler().to(HealthCheckHandler.class);
          }
        }));
        expose(Undertow.class);
      }
    }
  }

}

