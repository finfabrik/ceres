package com.blokaly.ceres.chronicle;

import net.openhft.chronicle.queue.DumpQueueMain;

import java.io.FileNotFoundException;

public class DumpMain {
  public static void main(String[] args) throws FileNotFoundException {
    DumpQueueMain.dump("test_queue");
  }
}
