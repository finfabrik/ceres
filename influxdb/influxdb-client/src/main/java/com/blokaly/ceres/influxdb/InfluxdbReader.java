package com.blokaly.ceres.influxdb;

import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BoundParameterQuery.QueryBuilder;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.List;
import java.util.stream.IntStream;

public class InfluxdbReader {
  private final InfluxDB influxDB;
  private final String database;



  public InfluxdbReader(InfluxDB influxDB, String database) {
    this.influxDB = influxDB;
    this.database = database;
  }

  public InfluxdbReaderBuilder prepareStatement(String query) {
    QueryBuilder builder = QueryBuilder.newQuery(query).forDatabase(database);
    return new InfluxdbReaderBuilder(builder);
  }

  public Table<String, String, Object> singleTable(Query query) throws InfluxdbQueryException {
    QueryResult result = influxDB.query(query);
    if (result.hasError()) {
      throw new InfluxdbQueryException(result.getError());
    } else {
      TreeBasedTable<String, String, Object> table = TreeBasedTable.create();
      List<QueryResult.Result> results = result.getResults();
      if (results.isEmpty()) {
        return table;
      } else {
        QueryResult.Result series = results.get(0);
        if (series.hasError()) {
          throw new InfluxdbQueryException(series.getError());
        } else {
          List<QueryResult.Series> rows = series.getSeries();
          if (rows==null || rows.isEmpty()) {
            return  table;
          } else {
            rows.forEach(row -> {
              List<String> cols = row.getColumns();
              row.getValues().forEach(vals -> {
                String time = vals.get(0).toString();
                IntStream.range(1, vals.size()).forEach(idx -> {
                  String colName = cols.get(idx);
                  Object val = vals.get(idx);
                  table.put(time, colName, val);
                });
              });
            });
            return  table;
          }
        }
      }
    }
  }

  public static class InfluxdbReaderBuilder {
    private final QueryBuilder builder;

    private InfluxdbReaderBuilder(QueryBuilder builder) {
      this.builder = builder;
    }

    public InfluxdbReaderBuilder set(String property, Object value) {
      builder.bind(property, value);
      return this;
    }

    public Query build() {
      return builder.create();
    }
  }

  public static class InfluxdbQueryException extends Exception {

    private InfluxdbQueryException(String reason) {
      super(reason);
    }
  }
}
