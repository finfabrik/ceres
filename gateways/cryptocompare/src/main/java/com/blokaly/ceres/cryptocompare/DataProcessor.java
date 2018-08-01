package com.blokaly.ceres.cryptocompare;

import com.blokaly.ceres.binding.SingleThread;
import com.blokaly.ceres.common.PairSymbol;
import com.blokaly.ceres.cryptocompare.api.HistoricalDataService;
import com.blokaly.ceres.cryptocompare.api.CandleBar;
import com.blokaly.ceres.influxdb.InfluxdbReader;
import com.blokaly.ceres.influxdb.InfluxdbWriter;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Table;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
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
  private static final String MINUTEBAR_LAST_UPDATE_QUERY = "select  last(\"lastUpdated\") from \"" + MINUTEBAR_STATUS + "\" where venue=$exchange and sym=$symbol order by time";
  private static final String HOURBAR_DATA = "hourbar.data";
  private static final String HOURBAR_STATUS = "hourbar.status";
  private static final String HOURBAR_LAST_UPDATE_QUERY = "select  last(\"lastUpdated\") from \"" + HOURBAR_STATUS + "\" where venue=$exchange and sym=$symbol order by time";
  private static final String DAYBAR_DATA = "daybar.data";
  private static final String DAYBAR_STATUS = "daybar.status";
  private static final String DAYBAR_LAST_UPDATE_QUERY = "select  last(\"lastUpdated\") from \"" + DAYBAR_STATUS + "\" where venue=$exchange and sym=$symbol order by time";

  private final HistoricalDataService service;
  private final ScheduledExecutorService executor;
  private final InfluxdbWriter writer;
  private final InfluxdbReader reader;
  private final ListMultimap<String, String> symbols;
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
    this.symbols = MultimapBuilder.hashKeys().arrayListValues().build();

    Set<Map.Entry<String, ConfigValue>> configs = config.getConfig("symbols").entrySet();
    for (Map.Entry<String, ConfigValue> conf : configs) {
      String exchange = conf.getKey();
      Collection<String> syms = (Collection<String>)conf.getValue().unwrapped();
      for (String sym : syms) {
        symbols.put(exchange, sym);
      }
    }
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
      Set<Map.Entry<String, Collection<String>>> exSyms = symbols.asMap().entrySet();
      for (Map.Entry<String, Collection<String>> exSym : exSyms) {
        String exchange = exSym.getKey();
        for (String sym : exSym.getValue()) {
          process(exchange, sym);
        }
      }
      running = false;
    }
    executor.schedule(this::historicalTask, 15L, TimeUnit.MINUTES);
  }

  private void process(String exchange, String symbol) {

    if (!started) {
      LOGGER.info("Not started, skip processing {}", symbol);
      return;
    }
    processDayBar(exchange, symbol);
    processHourBar(exchange, symbol);
    processMinuteBar(exchange, symbol);
  }

  private void processMinuteBar(String venue, String symbol) {
    try {
      InfluxdbReader.InfluxdbReaderBuilder builder = reader.prepareStatement(MINUTEBAR_LAST_UPDATE_QUERY);
      builder.set("exchange", venue);
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
        CandleBar minuteBars = service.getHistoMinutesOfDay(venue, pair.getBase(), pair.getTerms(), begin.toLocalDate());
        if (minuteBars.isSuccess()) {
          CandleBar.Bar[] bars = minuteBars.getBars();
          List<Point> points = Arrays.stream(bars).map(bar -> toInfluxdbPoint(MINUTEBAR_DATA, bar, venue, symbol)).collect(Collectors.toList());
          writer.writeBatch(points);
          LocalDateTime lastTime = LocalDateTime.ofEpochSecond(minuteBars.getTimeTo(), 0, ZoneOffset.UTC);
          Point status = updateStatus(MINUTEBAR_STATUS, venue, symbol, lastTime);
          writer.write(status);
          LOGGER.info("Historical minute bars processed for {}:{}", venue, symbol);
          begin = lastTime;
        } else {
          LOGGER.error("Error getting history minute bars for {}:{}, {}", venue, symbol, minuteBars.getMessage());
          break;
        }
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to process history minute bar", ex);
    }
  }

  private void processHourBar(String venue, String symbol) {
    try {
      InfluxdbReader.InfluxdbReaderBuilder builder = reader.prepareStatement(HOURBAR_LAST_UPDATE_QUERY);
      builder.set("exchange", venue);
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
        CandleBar hourBars = service.getHistoHoursOfDay(venue, pair.getBase(), pair.getTerms(), begin.toLocalDate());
        if (hourBars.isSuccess()) {
          CandleBar.Bar[] bars = hourBars.getBars();
          List<Point> points = Arrays.stream(bars).map(bar -> toInfluxdbPoint(HOURBAR_DATA, bar, venue, symbol)).collect(Collectors.toList());
          writer.writeBatch(points);
          LocalDateTime lastTime = LocalDateTime.ofEpochSecond(hourBars.getTimeTo(), 0, ZoneOffset.UTC);
          Point status = updateStatus(HOURBAR_STATUS, venue, symbol, lastTime);
          writer.write(status);
          LOGGER.info("Historical hour bars processed for {}:{}", venue, symbol);
          begin = lastTime;
        } else {
          LOGGER.error("Error getting history hour bars for {}:{}, {}", venue, symbol, hourBars.getMessage());
          break;
        }
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to process history hour bars", ex);
    }
  }

  private void processDayBar(String venue, String symbol) {
    try {
      InfluxdbReader.InfluxdbReaderBuilder builder = reader.prepareStatement(DAYBAR_LAST_UPDATE_QUERY);
      builder.set("exchange", venue);
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
        CandleBar dayBars = service.getHistoDaysOfYear(venue, pair.getBase(), pair.getTerms(), begin.plusDays(365).toLocalDate());
        LocalDateTime lastUpdate = processDayBars(venue, symbol, dayBars);
        if (lastUpdate != null) {
          begin = lastUpdate;
        } else {
          break;
        }
      }

      long days = begin.until(startOfToday, ChronoUnit.DAYS);
      if (days > 0) {
        CandleBar dayBars = service.getHistoDay(venue, pair.getBase(), pair.getTerms(), startOfToday.toLocalDate(), days);
        processDayBars(venue, symbol, dayBars);
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to process history day bars", ex);
    }
  }

  private LocalDateTime processDayBars(String venue, String symbol, CandleBar dayBars) {
    if (dayBars.isSuccess()) {
      CandleBar.Bar[] bars = dayBars.getBars();
      List<Point> points = Arrays.stream(bars).filter(bar -> bar.getHigh()>0).map(bar -> toInfluxdbPoint(DAYBAR_DATA, bar, venue, symbol)).collect(Collectors.toList());
      writer.writeBatch(points);
      LocalDateTime lastTime = LocalDateTime.ofEpochSecond(dayBars.getTimeTo(), 0, ZoneOffset.UTC);
      Point status = updateStatus(DAYBAR_STATUS, venue, symbol, lastTime);
      writer.write(status);
      LOGGER.info("Historical day bars processed for {}:{}", venue, symbol);
      return lastTime;
    } else {
      LOGGER.error("Error getting history day bars for {}:{}, {}", venue, symbol, dayBars.getMessage());
      return null;
    }
  }

  private Point toInfluxdbPoint(String measurement, CandleBar.Bar bar, String venue, String symbol) {

    Point.Builder builder = Point.measurement(measurement)
        .time(bar.getTime(), TimeUnit.SECONDS)
        .tag("venue", venue)
        .tag("sym", symbol);
    builder.addField("open", bar.getOpen());
    builder.addField("high", bar.getHigh());
    builder.addField("low", bar.getLow());
    builder.addField("close", bar.getClose());
    builder.addField("volumeFrom", bar.getVolumeFrom());
    builder.addField("volumeTo", bar.getVolumeTo());
    return builder.build();
  }

  private Point updateStatus(String measurement, String venue, String symbol, LocalDateTime last) {
    Point.Builder builder = Point.measurement(measurement)
        .time(last.toEpochSecond(ZoneOffset.UTC), TimeUnit.SECONDS)
        .tag("venue", venue)
        .tag("sym", symbol);
    OffsetDateTime offsetDateTime = last.atOffset(ZoneOffset.UTC);
    builder.addField("lastUpdated", offsetDateTime.toString());
    return builder.build();
  }
}
