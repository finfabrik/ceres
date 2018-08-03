package com.blokaly.ceres.chronicle;

import com.blokaly.ceres.common.Triple;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.pool.BytesPool;
import net.openhft.chronicle.bytes.util.Compressions;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

public class OutputMain {
  private static final BytesPool BP = new BytesPool();

  public static void main(String[] args) {
    String path = "/opt/projects/github.com/finfabrik/ceres/test_data";
    SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).rollCycle(RollCycles.LARGE_HOURLY).build();
    long total = queue.entryCount();
    System.out.println("total entries: " + total);
    ExcerptTailer tailer = queue.createTailer();

    long counter = 0;
    while (true) {
      Bytes bytes = BP.acquireBytes();
      boolean read = tailer.readBytes(bytes);
      if (read) {
        counter++;
        Triple<PayloadType, Long, String> decoded = decompress(bytes);
        System.out.println("decoded[" + counter + "/" + total + "]: " + decoded);
      }
      else {
        int queueCycle = queue.cycle();
        int tailerCycle = tailer.cycle();
        if (tailerCycle != queueCycle) {
          long index = queue.rollCycle().toIndex(queueCycle, 0);
          tailer.moveToIndex(index);
        } else {
          break;
        }
      }
    }
  }

  private static Triple<PayloadType, Long, String> decompress(Bytes bytes) {
    byte type = bytes.readByte();
    PayloadType payloadType = PayloadType.parse(type);
    long time = bytes.readLong();

    switch (payloadType) {
      case BEGIN:
      case END:
        return new Triple<PayloadType, Long, String>(payloadType, time, null);
      default: {
        byte[] uncompress = Compressions.Snappy.uncompress(bytes.bytesForRead().toByteArray());
        return new Triple<PayloadType, Long, String>(payloadType, time, new String(uncompress));
      }
    }
  }

}

