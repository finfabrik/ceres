package com.blokaly.ceres.cryptocompare.api;

import com.blokaly.ceres.network.RestGetJson;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;

@Singleton
public class HistoricalDataService {
  private static final Logger LOGGER = LoggerFactory.getLogger(HistoricalDataService.class);
  private static final String DEFAULT_VENUE = "CCCAGG";
  private static final String HISTO_MINUTE = "/histominute";
  private static final String HISTO_HOUR = "/histohour";
  private static final String HISTO_DAY = "/histoday";
  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final long MINUTES_OF_DAY = TimeUnit.DAYS.toMinutes(1);
  private static final long HOURS_OF_DAY = TimeUnit.DAYS.toHours(1);
  private final String apiPrefix;
  private final String apiAppName;
  private final Gson gson;
  private final RateLimiter rateLimiter;

  @Inject
  public HistoricalDataService(Config config, Gson gson) {
    apiPrefix = config.getString("api.url");
    apiAppName = config.hasPath("api.app") ? config.getString("api.app") : null;
    rateLimiter = RateLimiter.create(config.getInt("api.limit"));
    this.gson = gson;
  }

  public CandleBar getHistoMinutesOfDay(String venue, String base, String terms, LocalDate date) {
    LocalDate today = LocalDate.now(UTC);
    if (date.isAfter(today) || date.isBefore(today.minusDays(7))) {
      throw new IllegalArgumentException("Date must be in the last week, but got " + date);
    }

    ZonedDateTime endTime = date.atStartOfDay(UTC).plusDays(1);
    return getHistoMinute(venue, base, terms, endTime.toLocalDateTime(), MINUTES_OF_DAY);
  }

  public CandleBar getHistoMinute(String venue, String base, String terms, LocalDateTime toUtc, long limit) {
    long now = LocalDateTime.now(UTC).truncatedTo(ChronoUnit.MINUTES).toEpochSecond(ZoneOffset.UTC);
    long end = toUtc.truncatedTo(ChronoUnit.MINUTES).toEpochSecond(ZoneOffset.UTC);
    if (now <= end) {
      long secondsPerMinute = TimeUnit.MINUTES.toSeconds(1);
      end = now - secondsPerMinute;
      long startOfDay = LocalDate.now(UTC).atStartOfDay(UTC).toEpochSecond();
      limit = ( end - startOfDay ) / secondsPerMinute;
    }
    LinkedHashMap<String, String> params = getParams(venue, base, terms, end, limit);
    String res = dispatchRequest(apiPrefix + HISTO_MINUTE, params);
    LOGGER.debug("HistoMinute result: {}", res);
    if (res == null) {
      return CandleBar.fail("null response");
    } else {
      return gson.fromJson(res, CandleBar.class);
    }
  }

  public CandleBar getHistoHoursOfDay(String venue, String base, String terms, LocalDate date) {
    LocalDate today = LocalDate.now(UTC);
    if (date.isAfter(today)) {
      date = today;
    }

    ZonedDateTime endTime = date.atStartOfDay(UTC).plusDays(1);
    return getHistoHour(venue, base, terms, endTime.toLocalDateTime(), HOURS_OF_DAY);
  }

  public CandleBar getHistoHour(String venue, String base, String terms, LocalDateTime toUtc, long limit) {
    long now = LocalDateTime.now(UTC).truncatedTo(ChronoUnit.HOURS).toEpochSecond(ZoneOffset.UTC);
    long end = toUtc.truncatedTo(ChronoUnit.HOURS).toEpochSecond(ZoneOffset.UTC);
    if (now <= end) {
      long secondsPerHour = TimeUnit.HOURS.toSeconds(1);
      end = now - secondsPerHour;
      long startOfDay = LocalDate.now(UTC).atStartOfDay(UTC).toEpochSecond();
      limit = ( end - startOfDay ) / secondsPerHour;
    }
    LinkedHashMap<String, String> params = getParams(venue, base, terms, end, limit);
    String res = dispatchRequest(apiPrefix + HISTO_HOUR, params);
    LOGGER.debug("HistoHour result: {}", res);
    if (res == null) {
      return CandleBar.fail("null response");
    } else {
      return gson.fromJson(res, CandleBar.class);
    }
  }

  public CandleBar getHistoDaysOfYear(String venue, String base, String terms, LocalDate date) {
    return getHistoDay(venue, base, terms, date, 365);
  }

  public CandleBar getHistoDay(String venue, String base, String terms, LocalDate date, long limit) {
    LocalDate today = LocalDate.now().atStartOfDay(UTC).toLocalDate();
    if (date.isAfter(today)) {
      date = today;
    }

    LocalDate startOfDay = date.atStartOfDay(UTC).toLocalDate();
    if (!startOfDay.isEqual(date)) {
      date = startOfDay;
    }

    long end = date.atStartOfDay(UTC).toEpochSecond();
    LinkedHashMap<String, String> params = getParams(venue, base, terms, end, limit);
    String res = dispatchRequest(apiPrefix + HISTO_DAY, params);
    LOGGER.debug("HistoDay result: {}", res);
    if (res == null) {
      return CandleBar.fail("null response");
    } else {
      return gson.fromJson(res, CandleBar.class);
    }

  }

  private LinkedHashMap<String, String> getParams(String venue, String base, String terms, long end, long limit) {
    LinkedHashMap<String, String> params = new LinkedHashMap<>();
    if (!DEFAULT_VENUE.equals(venue)) {
      params.put("e", venue);
    }
    params.put("fsym", base.toUpperCase());
    params.put("tsym", terms.toUpperCase());
    params.put("toTs", String.valueOf(end));
    params.put("limit", String.valueOf(limit));
    if (apiAppName != null) {
      params.put("extraParams", apiAppName);
    }
    return params;
  }

  private String dispatchRequest(String url, LinkedHashMap<String, String> params) {
    if (rateLimiter.tryAcquire(1, TimeUnit.MINUTES)) {
      return RestGetJson.request(url, params);
    } else {
      LOGGER.error("Waiting for limit over 1 minute, giving up...");
      return null;
    }
  }
}
