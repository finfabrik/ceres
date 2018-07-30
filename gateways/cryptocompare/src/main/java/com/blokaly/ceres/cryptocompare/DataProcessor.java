package com.blokaly.ceres.cryptocompare;

import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.common.PairSymbol;
import com.blokaly.ceres.cryptocompare.api.HistoricalDataService;
import com.blokaly.ceres.cryptocompare.api.CandleBar;
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
  private static final String MINUTEBAR_LAST_UPDATE_QUERY = "select  last(\"lastUpdated\") from \"" + MINUTEBAR_STATUS + "\" where sym=$symbol order by time";
  private static final String HOURBAR_DATA = "hourbar.data";
  private static final String HOURBAR_STATUS = "hourbar.status";
  private static final String HOURBAR_LAST_UPDATE_QUERY = "select  last(\"lastUpdated\") from \"" + HOURBAR_STATUS + "\" where sym=$symbol order by time";
  private static final String DAYBAR_DATA = "daybar.data";
  private static final String DAYBAR_STATUS = "daybar.status";
  private static final String DAYBAR_LAST_UPDATE_QUERY = "select  last(\"lastUpdated\") from \"" + DAYBAR_STATUS + "\" where sym=$symbol order by time";

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
    processDayBar(symbol);
    processHourBar(symbol);
    processMinuteBar(symbol);
  }

  private void processMinuteBar(String symbol) {
    try {
      InfluxdbReader.InfluxdbReaderBuilder builder = reader.prepareStatement(MINUTEBAR_LAST_UPDATE_QUERY);
      builder.set("symbol", symbol);
      Table<String, String, Object> rs = this.reader.singleTable(builder.build());

      LocalDateTime begin;
      if (!rs.isEmpty()) {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse((String) rs.values().toArray()[0]);
        begin = zonedDateTime.toLocalDateTime();
      } else {
        begin = LocalDate.now(UTC).minusDays(7).atStartOfDay();
      }

      PairSymbol pair = PairSymbol.parse(symbol, "/");
      LocalDateTime startOfToday = LocalDate.now(UTC).atStartOfDay();
      while (begin.isBefore(startOfToday)) {
        CandleBar minuteBars = service.getHistoMinutesOfDay(pair.getBase(), pair.getTerms(), begin.toLocalDate());
        if (minuteBars.isSuccess()) {
          CandleBar.Bar[] bars = minuteBars.getBars();
          List<Point> points = Arrays.stream(bars).map(bar -> toInfluxdbPoint(MINUTEBAR_DATA, bar, symbol)).collect(Collectors.toList());
          writer.writeBatch(points);
          LocalDateTime lastTime = LocalDateTime.ofEpochSecond(minuteBars.getTimeTo(), 0, ZoneOffset.UTC);
          Point status = updateStatus(MINUTEBAR_STATUS, symbol, lastTime);
          writer.write(status);
          LOGGER.info("Historical minute bars processed for {}", symbol);
          begin = lastTime;
        } else {
          LOGGER.error("Error getting history minute bars for {}, {}", symbol, minuteBars.getMessage());
          break;
        }
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to process history minute bar", ex);
    }
  }

  private void processHourBar(String symbol) {
    try {
      InfluxdbReader.InfluxdbReaderBuilder builder = reader.prepareStatement(HOURBAR_LAST_UPDATE_QUERY);
      builder.set("symbol", symbol);
      Table<String, String, Object> rs = this.reader.singleTable(builder.build());

      LocalDateTime begin;
      if (!rs.isEmpty()) {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse((String) rs.values().toArray()[0]);
        begin = zonedDateTime.toLocalDateTime();
      } else {
        begin = LocalDate.now(UTC).minusDays(100).atStartOfDay();
      }

      PairSymbol pair = PairSymbol.parse(symbol, "/");
      LocalDateTime startOfToday = LocalDate.now(UTC).atStartOfDay();
      while (begin.isBefore(startOfToday)) {
        CandleBar hourBars = service.getHistoHoursOfDay(pair.getBase(), pair.getTerms(), begin.toLocalDate());
        if (hourBars.isSuccess()) {
          CandleBar.Bar[] bars = hourBars.getBars();
          List<Point> points = Arrays.stream(bars).map(bar -> toInfluxdbPoint(HOURBAR_DATA, bar, symbol)).collect(Collectors.toList());
          writer.writeBatch(points);
          LocalDateTime lastTime = LocalDateTime.ofEpochSecond(hourBars.getTimeTo(), 0, ZoneOffset.UTC);
          Point status = updateStatus(HOURBAR_STATUS, symbol, lastTime);
          writer.write(status);
          LOGGER.info("Historical hour bars processed for {}", symbol);
          begin = lastTime;
        } else {
          LOGGER.error("Error getting history hour bars for {}, {}", symbol, hourBars.getMessage());
          break;
        }
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to process history hour bars", ex);
    }
  }

  private void processDayBar(String symbol) {
    try {
      InfluxdbReader.InfluxdbReaderBuilder builder = reader.prepareStatement(DAYBAR_LAST_UPDATE_QUERY);
      builder.set("symbol", symbol);
      Table<String, String, Object> rs = this.reader.singleTable(builder.build());

      LocalDateTime begin;
      if (!rs.isEmpty()) {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse((String) rs.values().toArray()[0]);
        begin = zonedDateTime.toLocalDateTime();
      } else {
        begin = LocalDate.now(UTC).minusDays(365*5).atStartOfDay();
      }

      PairSymbol pair = PairSymbol.parse(symbol, "/");
      LocalDateTime startOfToday = LocalDate.now(UTC).atStartOfDay();
      while (begin.isBefore(startOfToday.minusDays(365))) {
        CandleBar dayBars = service.getHistoDaysOfYear(pair.getBase(), pair.getTerms(), begin.plusDays(365).toLocalDate());
        LocalDateTime lastUpdate = processDayBars(symbol, dayBars);
        if (lastUpdate != null) {
          begin = lastUpdate;
        } else {
          break;
        }
      }

      long days = begin.until(startOfToday, ChronoUnit.DAYS);
      if (days > 0) {
        CandleBar dayBars = service.getHistoDay(pair.getBase(), pair.getTerms(), startOfToday.toLocalDate(), days);
        processDayBars(symbol, dayBars);
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to process history day bars", ex);
    }
  }

  private LocalDateTime processDayBars(String symbol, CandleBar dayBars) {
    if (dayBars.isSuccess()) {
      CandleBar.Bar[] bars = dayBars.getBars();
      List<Point> points = Arrays.stream(bars).filter(bar -> bar.getHigh()>0).map(bar -> toInfluxdbPoint(DAYBAR_DATA, bar, symbol)).collect(Collectors.toList());
      writer.writeBatch(points);
      LocalDateTime lastTime = LocalDateTime.ofEpochSecond(dayBars.getTimeTo(), 0, ZoneOffset.UTC);
      Point status = updateStatus(DAYBAR_STATUS, symbol, lastTime);
      writer.write(status);
      LOGGER.info("Historical day bars processed for {}", symbol);
      return lastTime;
    } else {
      LOGGER.error("Error getting history day bars for {}, {}", symbol, dayBars.getMessage());
      return null;
    }
  }

  private Point toInfluxdbPoint(String measurement, CandleBar.Bar bar, String symbol) {

    Point.Builder builder = Point.measurement(measurement)
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

  private Point updateStatus(String measurement, String symbol, LocalDateTime last) {
    Point.Builder builder = Point.measurement(measurement)
        .time(last.toEpochSecond(ZoneOffset.UTC), TimeUnit.SECONDS)
        .tag("sym", symbol);
    OffsetDateTime offsetDateTime = last.atOffset(ZoneOffset.UTC);
    builder.addField("lastUpdated", offsetDateTime.toString());
    return builder.build();
  }
}
