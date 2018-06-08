package com.blokaly.ceres.bitstamp;

import com.blokaly.ceres.binding.BootstrapService;
import com.blokaly.ceres.binding.CeresModule;
import com.blokaly.ceres.bitstamp.event.DiffBookEvent;
import com.blokaly.ceres.bitstamp.event.OrderBookEvent;
import com.blokaly.ceres.common.*;
import com.blokaly.ceres.data.SymbolFormatter;
import com.blokaly.ceres.kafka.HBProducer;
import com.blokaly.ceres.kafka.KafkaCommonModule;
import com.blokaly.ceres.kafka.KafkaStreamModule;
import com.blokaly.ceres.kafka.ToBProducer;
import com.blokaly.ceres.orderbook.PriceBasedOrderBook;
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
import com.pusher.client.Pusher;
import com.pusher.client.PusherOptions;
import com.typesafe.config.Config;
import io.undertow.Undertow;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class BitstampService extends BootstrapService {
  private final List<PusherClient> clients;
  private final KafkaStreams streams;
  private final Undertow undertow;

  @Inject
  public BitstampService(List<PusherClient> clients,
                         @Named("Throttled") KafkaStreams streams,
                         Undertow undertow) {
    this.clients = clients;
    this.streams = streams;
    this.undertow = undertow;
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("starting pusher client...");
    clients.forEach(PusherClient::start);

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

    LOGGER.info("stopping pusher client...");
    clients.forEach(PusherClient::stop);

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
    public List<PusherClient> providePusherClients(Config config, Gson gson, ToBProducer producer, @SingleThread Provider<ExecutorService> provider) {
      PusherOptions options = new PusherOptions();
      String source = Source.valueOf(config.getString(CommonConfigs.APP_SOURCE).toUpperCase()).getCode();
      return config.getConfig("symbols").entrySet().stream()
          .map(item -> {
            String symbol = SymbolFormatter.normalise(item.getKey());
            OrderBookHandler handler = new OrderBookHandler(new PriceBasedOrderBook(symbol, symbol + "." + source), producer, gson, provider.get());
            String subId = (String) item.getValue().unwrapped();
            return new PusherClient(new Pusher(subId, options), handler, gson);
          })
          .collect(Collectors.toList());
    }

    @Provides
    @Singleton
    public Gson provideGson() {
      GsonBuilder builder = new GsonBuilder();
      builder.registerTypeAdapter(OrderBookEvent.class, new OrderBookEventAdapter());
      builder.registerTypeAdapter(DiffBookEvent.class, new DiffBookEventAdapter());
      return builder.create();
    }
  }

  public static void main(String[] args) {
    Services.start(new BitstampModule());
  }
}
