package com.blokaly.ceres.influxdb.ringbuffer;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

import java.util.function.Consumer;

@Singleton
public class BatchedPointsPublisher {
  private final RingBuffer<PointBuilderFactory.BatchedPointBuilder> ringBuffer;

  @Inject
  public BatchedPointsPublisher(Disruptor<PointBuilderFactory.BatchedPointBuilder> disruptor) {
    ringBuffer = disruptor.getRingBuffer();
  }

  public void publish(Consumer<PointBuilderFactory.BatchedPointBuilder> populator) {
    long sequence = ringBuffer.next();
    populator.accept(ringBuffer.get(sequence));
    ringBuffer.publish(sequence);
  }
}
