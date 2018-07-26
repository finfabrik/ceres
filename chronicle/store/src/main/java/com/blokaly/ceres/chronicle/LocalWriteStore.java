package com.blokaly.ceres.chronicle;

import com.blokaly.ceres.chronicle.ringbuffer.StringPayload;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalWriteStore implements WriteStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalWriteStore.class);
  private final RingBuffer<StringPayload> ringBuffer;
  private final String path;

  private static final EventTranslatorTwoArg<StringPayload, PayloadType, String> TRANSLATOR =
      (event, sequence, type, msg) -> event.setPayload(type, msg);

  public LocalWriteStore(RingBuffer<StringPayload> ringBuffer, String path) {
    this.ringBuffer = ringBuffer;
    this.path = path;
  }

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public void save(PayloadType type, String msg) {
    ringBuffer.publishEvent(TRANSLATOR, type, msg);
  }

}
