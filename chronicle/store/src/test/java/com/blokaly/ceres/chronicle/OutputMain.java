package com.blokaly.ceres.chronicle;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.pool.BytesPool;
import net.openhft.chronicle.bytes.util.Compressions;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

public class OutputMain {
  private static final BytesPool BP = new BytesPool();

  public static void main(String[] args) {
    String path = "/opt/projects/github.com/finfabrik/ceres/binance_marketdata";
    SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).build();
    ExcerptTailer tailer = queue.createTailer();
    long counter = 0;

    while (true) {
      Bytes bytes = BP.acquireBytes();
      boolean read = tailer.readBytes(bytes);
      if (read) {
        counter++;
        System.out.println("decoded[" + counter + "]: " + decompress(bytes));
      }
      else {
        Jvm.pause(10);
      }
    }
  }

  private static String decompress(Bytes bytes) {
    byte type = bytes.readByte();
    PayloadType payloadType = PayloadType.parse(type);
    long time = bytes.readLong();
    System.out.println("time: " + time + ", type: " + payloadType);
    if (payloadType == PayloadType.JSON) {
      byte[] uncompress = Compressions.Snappy.uncompress(bytes.bytesForRead().toByteArray());
      return new String(uncompress);
    } else {
      return null;
    }
  }
}

