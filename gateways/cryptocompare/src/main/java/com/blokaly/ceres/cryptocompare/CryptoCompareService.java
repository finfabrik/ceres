package com.blokaly.ceres.cryptocompare;

import com.blokaly.ceres.binding.BootstrapService;
import com.blokaly.ceres.binding.CeresModule;
import com.blokaly.ceres.cryptocompare.api.HistoricalData;
import com.blokaly.ceres.cryptocompare.api.MinuteBars;
import com.blokaly.ceres.system.Services;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.inject.Exposed;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

public class CryptoCompareService extends BootstrapService {

  private final HistoricalData historicalData;

  @Inject
  public CryptoCompareService(HistoricalData historicalData) {
    this.historicalData = historicalData;
  }

  @Override
  protected void startUp() throws Exception {
    LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
    MinuteBars bars = historicalData.getHistoMinute("BTC", "USD", now, 10);
  }


  @Override
  protected void shutDown() throws Exception {

  }

  public static class CryptoCompareModule extends CeresModule {

    @Override
    protected void configure() {
      MapBinder<Class, JsonDeserializer> binder = MapBinder.newMapBinder(binder(), Class.class, JsonDeserializer.class);
      binder.addBinding(MinuteBars.class).to(MinuteBars.EventAdapter.class);
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
    Services.start(new CryptoCompareModule());
  }
}
