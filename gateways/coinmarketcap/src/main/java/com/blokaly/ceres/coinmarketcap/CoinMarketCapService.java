package com.blokaly.ceres.coinmarketcap;

import com.blokaly.ceres.binding.BootstrapService;
import com.blokaly.ceres.binding.CeresModule;
import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.kafka.KafkaCommonModule;
import com.blokaly.ceres.kafka.StringProducer;
import com.blokaly.ceres.system.Services;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.inject.Exposed;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CoinMarketCapService extends BootstrapService {
  private final TickerRequester requester;
  private final RefRateProducer producer;
  private final Gson gson;
  private final ScheduledExecutorService ses;

  @Inject
  public CoinMarketCapService(TickerRequester requester, RefRateProducer producer, Gson gson, @SingleThread ScheduledExecutorService ses) {
    this.requester = requester;
    this.producer = producer;
    this.gson = gson;
    this.ses = ses;
  }

  @Override
  protected void startUp() throws Exception {
    ses.scheduleAtFixedRate(()->{
      TickerEvent[] tickers = requester.request(gson);
      producer.update(tickers);
    }, 0L, 5L, TimeUnit.MINUTES);
    ses.scheduleAtFixedRate(producer::publishRate, 5L, 5L, TimeUnit.SECONDS);
    awaitTerminated();
  }

  public static class CoinMarketCapModule extends CeresModule {

    @Override
    protected void configure() {
      install(new KafkaCommonModule());
      bindExpose(StringProducer.class);

      bindExpose(TickerRequester.class);
      bindExpose(RefRateProducer.class);

      MapBinder<Class, JsonDeserializer> binder = MapBinder.newMapBinder(binder(), Class.class, JsonDeserializer.class);
      binder.addBinding(TickerEvent.class).to(TickerEvent.EventAdapter.class);
    }

    @Exposed
    @Provides
    @Singleton
    public Gson provideGson(Map<Class, JsonDeserializer> deserializers) {
      GsonBuilder builder = new GsonBuilder();
      deserializers.forEach(builder::registerTypeAdapter);
      return builder.create();
    }
  }

  public static void main(String[] args) {
    Services.start(new CoinMarketCapModule());
  }
}
