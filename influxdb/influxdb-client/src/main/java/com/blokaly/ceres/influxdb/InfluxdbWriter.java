package com.blokaly.ceres.influxdb;

import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.Arrays;
import java.util.Collection;

public class InfluxdbWriter {

    private final InfluxDB influxDB;
    private final String database;
    private final String retention;

    public InfluxdbWriter(InfluxDB influxDB, String database) {
        this(influxDB, database, null);
    }

    public InfluxdbWriter(InfluxDB influxDB, String database, String retention) {
        this.influxDB = influxDB;
        this.database = database;
        this.retention = retention;
    }

    public void write(Point point) {
        influxDB.write(database, retention, point);
    }

    public void writeBatch(Point[] points) {
        this.writeBatch(Arrays.asList(points));
    }

    public void writeBatch(Collection<Point> points) {
        BatchPoints.Builder builder = BatchPoints.database(this.database).consistency(InfluxDB.ConsistencyLevel.ALL);
        points.forEach(builder::point);
        influxDB.write(builder.build());
    }
}
