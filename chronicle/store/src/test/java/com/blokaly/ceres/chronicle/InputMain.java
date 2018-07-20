package com.blokaly.ceres.chronicle;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.util.Compressions;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

import java.util.Random;
import java.util.Scanner;

public class InputMain {

  public static void main(String[] args) {
    String path = "test_queue";
    SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).rollCycle(RollCycles.LARGE_HOURLY).build();
    ExcerptAppender appender = queue.acquireAppender();
    char[] types = new char[]{'a', 'b', 'c'};
    Random rand = new Random();
    Bytes<Void> direct = Bytes.allocateElasticDirect(1024);
    Scanner read = new Scanner(System.in);
    while (true) {
      System.out.println("Input a string:");
      String line = read.nextLine();
      if (line.isEmpty()) break;

      char type = types[rand.nextInt(3)];
      System.out.println("type: " + type);
      long now = System.currentTimeMillis();
      direct.writeByte((byte)type);
      direct.writeLong(System.currentTimeMillis());
      direct.write(Compressions.Snappy.compress(line.getBytes()));
      appender.writeBytes(direct);
      direct.clear();
    }
    System.out.println("... bye.");
  }

  private static Bytes<byte[]> compress(String jsonString) {
    byte[] bytes = Compressions.Snappy.compress(jsonString.getBytes());
    return Bytes.wrapForRead(bytes);
  }
}
