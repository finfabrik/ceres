package com.blokaly.ceres.binance;

import com.blokaly.ceres.binance.event.DiffBookEvent;
import com.blokaly.ceres.binance.event.OrderBookEvent;
import com.blokaly.ceres.binding.BootstrapService;
import com.blokaly.ceres.binding.CeresModule;
import com.blokaly.ceres.system.Services;
import com.blokaly.ceres.kafka.HBProducer;
import com.blokaly.ceres.kafka.KafkaCommonModule;
import com.blokaly.ceres.kafka.KafkaStreamModule;
import com.blokaly.ceres.kafka.ToBProducer;
import com.blokaly.ceres.web.HandlerModule;
import com.blokaly.ceres.web.UndertowModule;
import com.blokaly.ceres.web.handlers.HealthCheckHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.undertow.Undertow;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Collection;

public class BinanceService extends BootstrapService {
  private final BinanceClientProvider provider;
  private final KafkaStreams streams;
  private final Undertow undertow;

  @Inject
  public BinanceService(BinanceClientProvider provider,
                        @Named("Throttled") KafkaStreams streams,
                        Undertow server) {
    this.provider = provider;
    this.streams = streams;
    this.undertow = server;
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("starting websocket clients...");
    provider.start();

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

    LOGGER.info("stopping websocket client...");
    provider.stop();

    LOGGER.info("stopping kafka streams...");
    streams.close();
  }

  public static class BinanceModule extends CeresModule {

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
      bindExpose(BinanceClientProvider.class);
      bind(new TypeLiteral<Collection<BinanceClient>>(){}).toProvider(BinanceClientProvider.class).asEagerSingleton();

      expose(StreamsBuilder.class).annotatedWith(Names.named("Throttled"));
      expose(KafkaStreams.class).annotatedWith(Names.named("Throttled"));
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
    Services.start(new BinanceModule());
  }
}