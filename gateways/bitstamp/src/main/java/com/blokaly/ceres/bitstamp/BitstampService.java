package com.blokaly.ceres.bitstamp;

import com.blokaly.ceres.binding.AwaitExecutionService;
import com.blokaly.ceres.binding.BootstrapService;
import com.blokaly.ceres.binding.CeresModule;
import com.blokaly.ceres.bitstamp.event.DiffBookEvent;
import com.blokaly.ceres.bitstamp.event.OrderBookEvent;
import com.blokaly.ceres.common.*;
import com.blokaly.ceres.data.SymbolFormatter;
import com.blokaly.ceres.influxdb.ringbuffer.BatchedPointsPublisher;
import com.blokaly.ceres.influxdb.ringbuffer.InfluxdbBufferModule;
import com.blokaly.ceres.kafka.HBProducer;
import com.blokaly.ceres.kafka.KafkaCommonModule;
import com.blokaly.ceres.kafka.KafkaStreamModule;
import com.blokaly.ceres.kafka.ToBProducer;
import com.blokaly.ceres.orderbook.PriceBasedOrderBook;
import com.blokaly.ceres.orderbook.TopOfBookProcessor;
import com.blokaly.ceres.system.CeresClock;
import com.blokaly.ceres.system.CommonConfigs;
import com.blokaly.ceres.system.Services;
import com.blokaly.ceres.binding.SingleThread;
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
import com.pusher.client.Pusher;
import com.pusher.client.PusherOptions;
import com.typesafe.config.Config;
import io.undertow.Undertow;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class BitstampService {

  private static boolean kafkaEnabled;
  private static boolean undertowEnabled;
  static {
    Config config = Configs.convertSystemUnderscoreToDot();
    kafkaEnabled = Configs.getOrDefault(config, "kafka.enabled", Configs.BOOLEAN_EXTRACTOR, false);
    undertowEnabled = Configs.getOrDefault(config, "undertow.enabled", Configs.BOOLEAN_EXTRACTOR, false);
  }

  public static class Client extends AwaitExecutionService {
    List<PusherClient> clients;

    @Inject
    public Client(List<PusherClient> clients) {
      this.clients = clients;
    }

    @Override
    protected void startUp() throws Exception {
      LOGGER.info("starting pusher client...");
      clients.forEach(PusherClient::start);
    }

    @Override
    protected void shutDown() throws Exception {
      LOGGER.info("stopping pusher client...");
      clients.forEach(PusherClient::stop);

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

  public static class BitstampModule extends CeresModule {

    @Override
    protected void configure() {
      configUndertow();
      configKafka();

      MapBinder<Class, JsonDeserializer> binder = MapBinder.newMapBinder(binder(), Class.class, JsonDeserializer.class);
      binder.addBinding(OrderBookEvent.class).to(OrderBookEvent.OrderBookEventAdapter.class);
      binder.addBinding(DiffBookEvent.class).to(DiffBookEvent.DiffBookEventAdapter.class);

      install(new InfluxdbBufferModule());
      bindExpose(BatchedPointsPublisher.class);
    }

    @Provides
    @Singleton
    @Exposed
    public List<PusherClient> providePusherClients(Config config,
                                                   CeresClock clock,
                                                   Gson gson,
                                                   TopOfBookProcessor producer,
                                                   TradesHandler tradesHandler,
                                                   @SingleThread Provider<ExecutorService> provider) {
      PusherOptions options = new PusherOptions();
      String source = Source.valueOf(config.getString(CommonConfigs.APP_SOURCE).toUpperCase()).getCode();
      return config.getConfig("symbols").entrySet().stream()
          .map(item -> {
            PairSymbol pair = PairSymbol.parse(item.getKey());
            String symbol = pair.getCode();
            OrderBookHandler handler = new OrderBookHandler(new PriceBasedOrderBook(symbol, symbol + "." + source), producer, gson, provider.get());
            String subId = (String) item.getValue().unwrapped();
            return new PusherClient(new Pusher(subId, options), clock, pair, handler, tradesHandler, gson);
          })
          .collect(Collectors.toList());
    }

    @Exposed
    @Provides
    @Singleton
    public Gson provideGson(Map<Class, JsonDeserializer> deserializers) {
      GsonBuilder builder = new GsonBuilder();
      deserializers.forEach(builder::registerTypeAdapter);
      return builder.create();
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
    Services.start(new BitstampModule());
  }
}
