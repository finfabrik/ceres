package com.blokaly.ceres.chronicle.ringbuffer;

import com.blokaly.ceres.chronicle.PayloadType;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.util.Compressions;
import net.openhft.chronicle.queue.ExcerptAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringPayload {
  private static final Logger LOGGER = LoggerFactory.getLogger(StringPayload.class);
  private final Bytes<Void> wrapper;

  public StringPayload(Bytes<Void> wrapper) {
    this.wrapper = wrapper;
  }

  public void setPayload(PayloadType type, String msg) {
    try {
      LOGGER.debug("storing [{}]{}", type, msg);
      wrapper.writeByte(type.byteType());
      wrapper.writeLong(System.currentTimeMillis());
      if (msg != null) {
        wrapper.write(compress(msg));
      }
    } catch (Exception ex) {
      LOGGER.error("failed to encode msg: " + msg, ex);
      wrapper.clear();
    }
  }

  public void writeBytes(ExcerptAppender appender) {
    try {
      appender.writeBytes(wrapper);
    } catch (Exception ex) {
      LOGGER.error("failed to persist msg", ex);
    } finally {
      wrapper.clear();
    }
  }

  private byte[] compress(String msg) {
    return Compressions.Snappy.compress(msg.getBytes());
  }
}
