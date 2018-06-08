package com.blokaly.ceres.huobi;

import com.blokaly.ceres.binding.BootstrapService;
import com.blokaly.ceres.binding.CeresModule;
import com.blokaly.ceres.network.WSConnectionListener;
import com.blokaly.ceres.system.CommonConfigs;
import com.blokaly.ceres.system.Services;
import com.blokaly.ceres.common.Source;
import com.blokaly.ceres.data.SymbolFormatter;
import com.blokaly.ceres.huobi.callback.*;
import com.blokaly.ceres.huobi.event.EventType;
import com.blokaly.ceres.huobi.event.WSEvent;
import com.blokaly.ceres.kafka.HBProducer;
import com.blokaly.ceres.kafka.KafkaCommonModule;
import com.blokaly.ceres.kafka.KafkaStreamModule;
import com.blokaly.ceres.kafka.ToBProducer;
import com.blokaly.ceres.orderbook.PriceBasedOrderBook;
import com.blokaly.ceres.web.HandlerModule;
import com.blokaly.ceres.web.UndertowModule;
import com.blokaly.ceres.web.handlers.HealthCheckHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.*;
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

public class HuobiService extends BootstrapService {
  private final HuobiClientProvider provider;
  private final KafkaStreams streams;
  private final Undertow undertow;

  @Inject
  public HuobiService(HuobiClientProvider provider,
                      @Named("Throttled") KafkaStreams streams,
                      Undertow undertow) {
    this.provider = provider;
    this.streams = streams;
    this.undertow = undertow;
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("starting huobi client...");
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

    LOGGER.info("stopping huobi client...");
    provider.stop();

    LOGGER.info("stopping kafka streams...");
    streams.close();
  }

  public static class HuobiModule extends CeresModule {

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

      bindAllCallbacks();
      bind(MessageHandler.class).to(MessageHandlerImpl.class).in(Singleton.class);
      bindExpose(HuobiClientProvider.class).asEagerSingleton();
      bind(WSConnectionListener.class).to(HuobiClientProvider.class);
      bindExpose(HuobiClient.class).toProvider(HuobiClientProvider.class);
    }

    @Provides
    @Exposed
    public URI provideUri(Config config) throws Exception {
      return new URI(config.getString("app.ws.url"));
    }

    @Provides
    @Singleton
    @Exposed
    public Gson provideGson(Map<EventType, CommandCallbackHandler> handlers) {
      GsonBuilder builder = new GsonBuilder();
      builder.registerTypeAdapter(WSEvent.class, new EventAdapter(handlers));
      return builder.create();
    }

    @Provides
    @Singleton
    @Exposed
    public Map<String, PriceBasedOrderBook> provideOrderBooks(Config config) {
      List<String> symbols = config.getStringList("symbols");
      String source = Source.valueOf(config.getString(CommonConfigs.APP_SOURCE).toUpperCase()).getCode();
      return symbols.stream().collect(Collectors.toMap(sym->sym, sym -> {
        String symbol = SymbolFormatter.normalise(sym);
        String pair = symbol.endsWith("usdt") ? symbol.replace("usdt", "usd") : symbol;
        return new PriceBasedOrderBook(symbol, pair + "." + source);
      }));
    }

    private void bindAllCallbacks() {
      MapBinder<EventType, CommandCallbackHandler> binder = MapBinder.newMapBinder(binder(), EventType.class, CommandCallbackHandler.class);
      binder.addBinding(EventType.PING).to(PingCallbackHandler.class);
      binder.addBinding(EventType.PONG).to(PongCallbackHandler.class);
      binder.addBinding(EventType.SUBSCRIBED).to(SubscribedCallbackHandler.class);
      binder.addBinding(EventType.TICK).to(SnapshotCallbackHandler.class);
    }
  }

  public static void main(String[] args) {
    Services.start(new HuobiModule());
  }
}