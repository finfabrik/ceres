package com.blokaly.ceres.influxdb.ringbuffer;

import com.blokaly.ceres.influxdb.InfluxdbWriter;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BatchedPointsHandler implements EventHandler<PointBuilderFactory.BatchedPointBuilder> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BatchedPointsHandler.class);
  private static final int MAX_BATCH_SIZE = 100;
  private static final long IDLE = 5000;
  private final InfluxdbWriter writer;
  private final List<Point> batch = new ArrayList<Point>();
  private long lastProcessed;

  public BatchedPointsHandler(InfluxDB influxDB, String database) {
    this.writer = new InfluxdbWriter(influxDB, database);;
  }


  @Override
  public void onEvent(PointBuilderFactory.BatchedPointBuilder builder, long sequence, boolean endOfBatch) throws Exception {
    long now = System.currentTimeMillis();
    Point point = builder.build();
    batch.add(point);
    builder.reset();
    if (batch.size() >= MAX_BATCH_SIZE || now - lastProcessed >= IDLE)
    {
      processBatch(now);
    }
  }

  private void processBatch(long time) {
    try {
      LOGGER.debug("processing influxdb batch points...");
      writer.writeBatch(batch);
      lastProcessed = time;
      batch.clear();
    } catch (Exception ex) {
      LOGGER.error("Failed to write batch to influxdb, batch size now: " + batch.size(), ex);
    }
  }
}
