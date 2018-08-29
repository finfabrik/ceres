package com.blokaly.ceres.binance;

import com.blokaly.ceres.binance.event.DiffBookEvent;
import com.blokaly.ceres.binance.event.OrderBookEvent;
import com.blokaly.ceres.binance.event.TradeEvent;
import com.blokaly.ceres.binding.AwaitExecutionService;
import com.blokaly.ceres.binding.BootstrapService;
import com.blokaly.ceres.binding.CeresModule;
import com.blokaly.ceres.chronicle.ChronicleStoreModule;
import com.blokaly.ceres.chronicle.WriteStore;
import com.blokaly.ceres.chronicle.ringbuffer.StringPayload;
import com.blokaly.ceres.common.Configs;
import com.blokaly.ceres.influxdb.ringbuffer.BatchedPointsPublisher;
import com.blokaly.ceres.influxdb.ringbuffer.InfluxdbBufferModule;
import com.blokaly.ceres.kafka.HBProducer;
import com.blokaly.ceres.kafka.KafkaCommonModule;
import com.blokaly.ceres.kafka.KafkaStreamModule;
import com.blokaly.ceres.kafka.ToBProducer;
import com.blokaly.ceres.orderbook.TopOfBookProcessor;
import com.blokaly.ceres.system.Services;
import com.blokaly.ceres.web.HandlerModule;
import com.blokaly.ceres.web.UndertowModule;
import com.blokaly.ceres.web.handlers.HealthCheckHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.inject.*;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.lmax.disruptor.dsl.Disruptor;
import com.typesafe.config.Config;
import io.undertow.Undertow;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Collection;
import java.util.Map;

public class BinanceService {

  private static boolean kafkaEnabled;
  private static boolean undertowEnabled;
  static {
    Config config = Configs.convertSystemUnderscoreToDot();
    kafkaEnabled = Configs.getOrDefault(config, "kafka.enabled", Configs.BOOLEAN_EXTRACTOR, true);
    undertowEnabled = Configs.getOrDefault(config, "undertow.enabled", Configs.BOOLEAN_EXTRACTOR, true);
  }

  public static class Client extends AwaitExecutionService {
    private final BinanceClientProvider provider;

    @Inject
    public Client(BinanceClientProvider provider) {
      this.provider = provider;
    }

    @Override
    protected void startUp() throws Exception {
      LOGGER.info("starting websocket clients...");
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

  public static class BinanceModule extends CeresModule {

    @Override
    protected void configure() {

      configUndertow();
      configKafka();
      configChronicle();

      MapBinder<Class, JsonDeserializer> binder = MapBinder.newMapBinder(binder(), Class.class, JsonDeserializer.class);
      binder.addBinding(OrderBookEvent.class).to(OrderBookEvent.Adapter.class);
      binder.addBinding(DiffBookEvent.class).to(DiffBookEvent.Adapter.class);
      binder.addBinding(TradeEvent.class).to(TradeEvent.Adapter.class);
      bindExpose(BinanceClientProvider.class);
      bind(new TypeLiteral<Collection<BinanceClient>>(){}).toProvider(BinanceClientProvider.class).asEagerSingleton();

      install(new InfluxdbBufferModule());
      bindExpose(BatchedPointsPublisher.class);
    }

    @Exposed
    @Provides
    @Singleton
    public Gson provideGson(Map<Class, JsonDeserializer> deserializers) {
      GsonBuilder builder = new GsonBuilder();
      deserializers.forEach(builder::registerTypeAdapter);
      return builder.create();
    }

    private void configChronicle() {
      install(new ChronicleStoreModule());
      TypeLiteral<Disruptor<StringPayload>> disruptorTypeLiteral = new TypeLiteral<Disruptor<StringPayload>>() {};
      expose(disruptorTypeLiteral);
      expose(SingleChronicleQueue.class);
      expose(WriteStore.class);
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

  public static void main(String[] args) {
    Services.start(new BinanceModule());
  }
}