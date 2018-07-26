package com.blokaly.ceres.chronicle;

import com.blokaly.ceres.binding.CeresModule;
import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.chronicle.ringbuffer.StringPayload;
import com.blokaly.ceres.chronicle.ringbuffer.StringPayloadFactory;
import com.blokaly.ceres.chronicle.ringbuffer.StringPayloadHandler;
import com.blokaly.ceres.common.Configs;
import com.google.inject.Exposed;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.lmax.disruptor.dsl.Disruptor;
import com.typesafe.config.Config;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

import java.util.concurrent.ExecutorService;

import static com.blokaly.ceres.common.Configs.STRING_EXTRACTOR;

public class ChronicleStoreModule extends CeresModule {
  private static final String STORE_PATH = "store.path";
  private static final String STORE_ROLL = "store.roll";
  private static final String HOURLY_ROLL = "LARGE_HOURLY";

  @Override
  protected void configure() {
    bindExpose(WriteStore.class).toProvider(WriteStoreProvider.class).in(Singleton.class);
  }

  @Provides
  @Singleton
  @Exposed
  public SingleChronicleQueue provideQueue(Config config) {
    String path = config.getString(STORE_PATH);
    String roll = Configs.getOrDefault(config, STORE_ROLL, STRING_EXTRACTOR, HOURLY_ROLL);
    RollCycles rollCycles = RollCycles.valueOf(roll);
    return SingleChronicleQueueBuilder.binary(path).rollCycle(rollCycles).build();
  }

  @Provides
  @Singleton
  @Exposed
  public Disruptor<StringPayload> provideDisruptor(Provider<SingleChronicleQueue> provider, @SingleThread ExecutorService executor) {
    StringPayloadFactory factory = new StringPayloadFactory();
    int bufferSize = 128;
    Disruptor<StringPayload> disruptor = new Disruptor<>(factory, bufferSize, executor);
    disruptor.handleEventsWith(new StringPayloadHandler(provider.get()));
    disruptor.start();
    return disruptor;
  }
}
