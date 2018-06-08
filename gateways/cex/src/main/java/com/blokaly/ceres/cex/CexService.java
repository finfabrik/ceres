package com.blokaly.ceres.cex;

import com.blokaly.ceres.binding.BootstrapService;
import com.blokaly.ceres.binding.CeresModule;
import com.blokaly.ceres.cex.callback.*;
import com.blokaly.ceres.cex.event.AbstractEvent;
import com.blokaly.ceres.cex.event.EventType;
import com.blokaly.ceres.network.WSConnectionListener;
import com.blokaly.ceres.system.CommonConfigs;
import com.blokaly.ceres.common.PairSymbol;
import com.blokaly.ceres.system.Services;
import com.blokaly.ceres.common.Source;
import com.blokaly.ceres.data.SymbolFormatter;
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

public class CexService extends BootstrapService {
  private final CexClientProvider provider;
  private final KafkaStreams streams;
  private final Undertow undertow;

  @Inject
  public CexService(CexClientProvider provider,
                    @Named("Throttled") KafkaStreams streams,
                    Undertow undertow) {
    this.provider = provider;
    this.streams = streams;
    this.undertow = undertow;
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("starting cex client...");
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

    LOGGER.info("stopping cex client...");
    provider.stop();

    LOGGER.info("stopping kafka streams...");
    streams.close();
  }

  public static class CexModule extends CeresModule {

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
      bindExpose(CexClientProvider.class).asEagerSingleton();
      bind(WSConnectionListener.class).to(CexClientProvider.class);
      bindExpose(CexClient.class).toProvider(CexClientProvider.class);
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
      builder.registerTypeAdapter(AbstractEvent.class, new EventAdapter(handlers));
      return builder.create();
    }

    @Provides
    @Singleton
    @Exposed
    public Map<PairSymbol, PriceBasedOrderBook> provideOrderBooks(Config config) {
      List<String> symbols = config.getStringList("symbols");
      String source = Source.valueOf(config.getString(CommonConfigs.APP_SOURCE).toUpperCase()).getCode();
      return symbols.stream().collect(Collectors.toMap(this::parseSymbol, sym -> {
        PairSymbol pairSymbol = parseSymbol(sym);
        String code = pairSymbol.getCode();
        return new PriceBasedOrderBook(code, code + "." + source);
      }));
    }

    private PairSymbol parseSymbol(String symbol) {
      String[] syms = symbol.split("/");
      return SymbolFormatter.normalise(syms[0], syms[1]);
    }

    private void bindAllCallbacks() {
      MapBinder<EventType, CommandCallbackHandler> binder = MapBinder.newMapBinder(binder(), EventType.class, CommandCallbackHandler.class);
      binder.addBinding(EventType.CONNECTED).to(ConnectedCallbackHandler.class);
      binder.addBinding(EventType.AUTH).to(AuthCallbackHandler.class);
      binder.addBinding(EventType.SUBSCRIBE).to(SnapshotCallbackHandler.class);
      binder.addBinding(EventType.UPDATE).to(MDUpdateCallbackHandler.class);
      binder.addBinding(EventType.PING).to(PingCallbackHandler.class);
    }
  }

  public static void main(String[] args) {
    Services.start(new CexModule());
  }
}
