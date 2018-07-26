package com.blokaly.ceres.chronicle.ringbuffer;

import com.lmax.disruptor.EventHandler;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

public class StringPayloadHandler implements EventHandler<StringPayload> {

  private final ExcerptAppender appender;

  public StringPayloadHandler(SingleChronicleQueue queue) {
    this.appender = queue.acquireAppender();
  }

  @Override
  public void onEvent(StringPayload event, long sequence, boolean endOfBatch) throws Exception {
    if (event != null) {
      event.writeBytes(appender);
    }
  }
}
