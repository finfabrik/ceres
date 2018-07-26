package com.blokaly.ceres.chronicle;

import com.blokaly.ceres.binding.ServiceProvider;
import com.blokaly.ceres.chronicle.ringbuffer.StringPayload;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.lmax.disruptor.dsl.Disruptor;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class WriteStoreProvider extends ServiceProvider<WriteStore> {
  private static final Logger LOGGER = LoggerFactory.getLogger(WriteStoreProvider.class);
  private final SingleChronicleQueue queue;
  private final WriteStore store;

  @Inject
  public WriteStoreProvider(Provider<SingleChronicleQueue> provider, Disruptor<StringPayload> disruptor) {
    this.queue = provider.get();
    store = new LocalWriteStore(disruptor.getRingBuffer(), queue.fileAbsolutePath());
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("Using local write store: {}", store.getPath());
    store.save(PayloadType.BEGIN, null);
  }

  @Override
  protected void shutDown() throws Exception {
    if (queue != null) {
      LOGGER.info("Closing local write store: {}", store.getPath());
      store.save(PayloadType.END, null);
      queue.close();
    }
  }

  @Override
  public WriteStore get() {
    return store;
  }
}
