package com.blokaly.ceres.chronicle;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.util.Compressions;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalWriteStore implements WriteStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalWriteStore.class);
  private final String path;
  private final ExcerptAppender appender;
  private final Bytes<Void> wrapper;

  public LocalWriteStore(String path, SingleChronicleQueue queue) {
    this.path = path;
    appender = queue.acquireAppender();
    wrapper = Bytes.allocateElasticDirect(1024);
  }

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public void save(PayloadType type, String msg) {
    try {
      LOGGER.debug("storing [{}]{}", type, msg);
      wrapper.writeByte(type.byteType());
      wrapper.writeLong(System.currentTimeMillis());
      if (msg != null) {
        wrapper.write(compress(msg));
      }
      appender.writeBytes(wrapper);
    } catch (Exception ex) {
      LOGGER.error("failed to store msg: " + msg, ex);
    } finally {
      wrapper.clear();
    }
  }

  private static byte[] compress(String msg) {
    return Compressions.Snappy.compress(msg.getBytes());
  }
}
