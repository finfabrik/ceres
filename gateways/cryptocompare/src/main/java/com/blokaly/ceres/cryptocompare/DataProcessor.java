package com.blokaly.ceres.cryptocompare;

import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.common.PairSymbol;
import com.blokaly.ceres.cryptocompare.api.HistoricalDataService;
import com.blokaly.ceres.cryptocompare.api.MinuteBars;
import com.blokaly.ceres.influxdb.InfluxdbReader;
import com.blokaly.ceres.influxdb.InfluxdbWriter;
import com.google.common.collect.Table;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Singleton
public class DataProcessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataProcessor.class);
  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final String INFLUXDB_DATABASE = "influxdb.database";
  private static final String MINUTEBAR_DATA = "minutebar.data";
  private static final String MINUTEBAR_STATUS = "minutebar.status";
  private static final String LAST_UPDATE_QUERY = "select  last(\"lastUpdated\") from \"" + MINUTEBAR_STATUS + "\" where sym=$symbol order by time";
  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("M/d/yyyy h:mm:ss a");
  private final HistoricalDataService service;
  private final ScheduledExecutorService executor;
  private final InfluxdbWriter writer;
  private final InfluxdbReader reader;
  private final List<String> symbols;
  private boolean running;
  private volatile boolean started;

  @Inject
  public DataProcessor(HistoricalDataService service,
                       InfluxDB influxDB,
                       Config config,
                       @SingleThread ScheduledExecutorService ses) {
    this.service = service;
    this.executor = ses;
    String database = config.getString(INFLUXDB_DATABASE);
    this.writer = new InfluxdbWriter(influxDB, database);
    this.reader = new InfluxdbReader(influxDB, database);
    this.symbols = config.getStringList("symbols");
    running = false;
    started = false;
  }

  public void start() {
    started = true;
    executor.execute(this::historicalTask);
  }

  public void stop() {
    started = false;
  }

  private void historicalTask() {
    if (!running) {
      running = true;
      for (String sym : symbols) {
        process(sym);
      }
      running = false;
    }
    executor.schedule(this::historicalTask, 5L, TimeUnit.MINUTES);
  }

  private void process(String symbol) {

    if (!started) {
      LOGGER.info("Not started, skip processing {}", symbol);
      return;
    }

    try {
      InfluxdbReader.InfluxdbReaderBuilder builder = reader.prepareStatement(LAST_UPDATE_QUERY);
      builder.set("symbol", symbol);

      LocalDateTime begin;
      Table<String, String, Object> rs = this.reader.singleTable(builder.build());
      if (!rs.isEmpty()) {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse((String) rs.values().toArray()[0]);
        begin = zonedDateTime.toLocalDateTime();
      } else {
        begin = LocalDate.now(UTC).minusDays(7).atStartOfDay();
      }

      PairSymbol pair = PairSymbol.parse(symbol, "/");
      LocalDateTime startOfToday = LocalDate.now(UTC).atStartOfDay();
      while (begin.isBefore(startOfToday)) {
        MinuteBars minuteBars = service.getHistoMinuteOfDay(pair.getBase(), pair.getTerms(), begin.toLocalDate());
        if (minuteBars.isSuccess()) {
          MinuteBars.Bar[] bars = minuteBars.getBars();
          List<Point> points = Arrays.stream(bars).map(bar -> toInfluxdbPoint(bar, symbol)).collect(Collectors.toList());
          writer.writeBatch(points);
          LocalDateTime lastTime = LocalDateTime.ofEpochSecond(minuteBars.getTimeTo(), 0, ZoneOffset.UTC);
          Point status = updateStatus(symbol, lastTime);
          writer.write(status);
          LOGGER.info("Historical minute bars processed for {}", symbol);
          begin = lastTime;
        } else {
          LOGGER.error("Error getting history minute bars for {}, {}", symbol, minuteBars.getMessage());
          break;
        }
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to process minute chart bar", ex);
    }

  }

  private Point toInfluxdbPoint(MinuteBars.Bar bar, String symbol) {

    Point.Builder builder = Point.measurement(MINUTEBAR_DATA)
        .time(bar.getTime(), TimeUnit.SECONDS)
        .tag("sym", symbol);
    builder.addField("open", bar.getOpen());
    builder.addField("high", bar.getHigh());
    builder.addField("low", bar.getLow());
    builder.addField("close", bar.getClose());
    builder.addField("volumeFrom", bar.getVolumeFrom());
    builder.addField("volumeTo", bar.getVolumeTo());
    return builder.build();
  }

  private Point updateStatus(String symbol, LocalDateTime last) {
    Point.Builder builder = Point.measurement(MINUTEBAR_STATUS)
        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
        .tag("sym", symbol);
    OffsetDateTime offsetDateTime = last.atOffset(ZoneOffset.UTC);
    builder.addField("lastUpdated", offsetDateTime.toString());
    return builder.build();
  }
}
