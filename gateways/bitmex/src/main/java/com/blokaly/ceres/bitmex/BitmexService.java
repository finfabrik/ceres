package com.blokaly.ceres.bitmex;

import com.blokaly.ceres.binding.AwaitExecutionService;
import com.blokaly.ceres.binding.CeresModule;
import com.blokaly.ceres.bitmex.event.Incremental;
import com.blokaly.ceres.bitmex.event.Snapshot;
import com.blokaly.ceres.bitmex.event.Trades;
import com.blokaly.ceres.common.Source;
import com.blokaly.ceres.data.SymbolFormatter;
import com.blokaly.ceres.influxdb.ringbuffer.BatchedPointsPublisher;
import com.blokaly.ceres.influxdb.ringbuffer.InfluxdbBufferModule;
import com.blokaly.ceres.network.WSConnectionListener;
import com.blokaly.ceres.orderbook.OrderBasedOrderBook;
import com.blokaly.ceres.system.CommonConfigs;
import com.blokaly.ceres.system.Services;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.inject.Exposed;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;
import com.typesafe.config.Config;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BitmexService {

  public static class Client extends AwaitExecutionService {
    private final BitmexClientProvider provider;

    @Inject
    public Client(BitmexClientProvider provider) {
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

  public static class BitmexModule extends CeresModule {

    @Override
    protected void configure() {

      MapBinder<Class, JsonDeserializer> binder = MapBinder.newMapBinder(binder(), Class.class, JsonDeserializer.class);
      binder.addBinding(Snapshot.class).to(Snapshot.Adapter.class);
      binder.addBinding(Incremental.class).to(Incremental.Adapter.class);
      binder.addBinding(Trades.class).to(Trades.Adapter.class);

      bind(MessageHandler.class).to(MessageHandlerImpl.class).in(Singleton.class);
      bindExpose(BitmexClientProvider.class).asEagerSingleton();
      bind(WSConnectionListener.class).to(BitmexClientProvider.class);
      bindExpose(BitmexClient.class).toProvider(BitmexClientProvider.class);

      install(new InfluxdbBufferModule());
      bindExpose(BatchedPointsPublisher.class);
    }

    @Provides
    @Exposed
    public URI provideUri(Config config) throws Exception {
      return new URI(config.getString("app.ws.url"));
    }

    @Exposed
    @Provides
    @Singleton
    public Gson provideGson(Map<Class, JsonDeserializer> deserializers) {
      GsonBuilder builder = new GsonBuilder();
      deserializers.forEach(builder::registerTypeAdapter);
      return builder.create();
    }

    @Provides
    @Singleton
    @Exposed
    public Map<String, OrderBasedOrderBook> provideOrderBooks(Config config) {
      List<String> symbols = config.getStringList("symbols");
      String source = Source.valueOf(config.getString(CommonConfigs.APP_SOURCE).toUpperCase()).getCode();
      return symbols.stream().collect(Collectors.toMap(sym -> sym, sym -> {
        String symbol = SymbolFormatter.normalise(sym);
        return new OrderBasedOrderBook(symbol, symbol + "." + source);
      }));
    }
  }

  public static void main(String[] args) {
    Services.start(new BitmexModule());
  }
}
