package com.blokaly.ceres.chronicle;

import com.blokaly.ceres.binding.ServiceProvider;
import com.blokaly.ceres.common.Configs;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.blokaly.ceres.common.Configs.STRING_EXTRACTOR;

@Singleton
public class WriteStoreProvider extends ServiceProvider<WriteStore> {
  private static final Logger LOGGER = LoggerFactory.getLogger(WriteStoreProvider.class);
  private static final String STORE_PATH = "store.path";
  private static final String STORE_ROLL = "store.roll";
  private static final String HOURLY_ROLL = "LARGE_HOURLY";
  private final WriteStore store;
  private final SingleChronicleQueue queue;

  @Inject
  public WriteStoreProvider(Config config) {
    if (config.hasPath(STORE_PATH)) {
      String path = config.getString(STORE_PATH);
      String roll = Configs.getOrDefault(config, STORE_ROLL, STRING_EXTRACTOR, HOURLY_ROLL);
      RollCycles rollCycles = RollCycles.valueOf(roll);
      queue = SingleChronicleQueueBuilder.binary(path).rollCycle(rollCycles).build();
      store = new LocalWriteStore(path, queue);
    } else {
      queue = null;
      store = new NoOpStore();
    }
  }

  @Override
  protected void startUp() throws Exception {
    if (store instanceof NoOpStore) {
      LOGGER.info("{} does not exist in config, using NoOp store", STORE_PATH);
    } else {
      LOGGER.info("Using local write store: {}", store.getPath());
      store.save(PayloadType.BEGIN, null);
    }
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

  private static class NoOpStore implements WriteStore {

    @Override
    public String getPath() {
      return null;
    }

    @Override
    public void save(PayloadType type, String msg) { }
  }
}
