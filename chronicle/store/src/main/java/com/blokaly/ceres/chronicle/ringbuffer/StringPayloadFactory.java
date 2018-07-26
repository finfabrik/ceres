package com.blokaly.ceres.chronicle.ringbuffer;

import com.lmax.disruptor.EventFactory;
import net.openhft.chronicle.bytes.Bytes;

public class StringPayloadFactory implements EventFactory<StringPayload> {

  private static final int INITIAL_CAPACITY = 1024;

  public StringPayload newInstance()
  {
    return new StringPayload(Bytes.allocateElasticDirect(INITIAL_CAPACITY));
  }

}
