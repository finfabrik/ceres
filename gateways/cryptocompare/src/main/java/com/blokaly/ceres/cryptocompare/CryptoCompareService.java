package com.blokaly.ceres.cryptocompare;

import com.blokaly.ceres.binding.AwaitExecutionService;
import com.blokaly.ceres.binding.CeresModule;
import com.blokaly.ceres.cryptocompare.api.CandleBar;
import com.blokaly.ceres.influxdb.InfluxdbModule;
import com.blokaly.ceres.system.Services;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.inject.Exposed;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;
import org.influxdb.InfluxDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CryptoCompareService extends AwaitExecutionService {
  private static final Logger LOGGER = LoggerFactory.getLogger(CryptoCompareService.class);
  private final DataProcessor processor;

  @Inject
  public CryptoCompareService(DataProcessor processor) {
    this.processor = processor;
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("Starting CryptoCompare data processor...");
    processor.start();
  }


  @Override
  protected void shutDown() throws Exception {
    LOGGER.info("Stopping CryptoCompare data processor...");
    processor.stop();
  }

  public static class CryptoCompareModule extends CeresModule {

    @Override
    protected void configure() {
      MapBinder<Class, JsonDeserializer> binder = MapBinder.newMapBinder(binder(), Class.class, JsonDeserializer.class);
      binder.addBinding(CandleBar.class).to(CandleBar.EventAdapter.class);

      install(new InfluxdbModule());
      expose(InfluxDB.class);
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
