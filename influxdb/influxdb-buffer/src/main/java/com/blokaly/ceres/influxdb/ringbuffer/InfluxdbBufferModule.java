package com.blokaly.ceres.influxdb.ringbuffer;

import com.blokaly.ceres.binding.CeresModule;
import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.influxdb.InfluxdbModule;
import com.google.inject.Exposed;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.lmax.disruptor.dsl.Disruptor;
import com.typesafe.config.Config;
import org.influxdb.InfluxDB;

import java.util.concurrent.ExecutorService;

public class InfluxdbBufferModule extends CeresModule {

  @Override
  protected void configure() {
    install(new InfluxdbModule());
  }

  @Provides
  @Singleton
  @Exposed
  public Disruptor<PointBuilderFactory.BatchedPointBuilder> provideDisruptor(Config config, InfluxDB influxDB, @SingleThread ExecutorService executor) {
    PointBuilderFactory factory = new PointBuilderFactory();
    int bufferSize = 128;
    Disruptor<PointBuilderFactory.BatchedPointBuilder> disruptor = new Disruptor<>(factory, bufferSize, executor);
    String database = config.getString("influxdb.database");
    disruptor.handleEventsWith(new BatchedPointsHandler(influxDB, database));
    disruptor.start();
    return disruptor;
  }
}