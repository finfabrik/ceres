package com.blokaly.ceres.influxdb;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

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
}
