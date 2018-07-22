package com.blokaly.ceres.cryptocompare.api;

import com.blokaly.ceres.network.RestGetJson;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;

@Singleton
public class HistoricalData {
  private static final Logger LOGGER = LoggerFactory.getLogger(HistoricalData.class);
  private static final String HISTO_MINUTE = "/histominute";
  private final String apiPrefix;
  private final String apiAppName;
  private final Gson gson;

  @Inject
  public HistoricalData(Config config, Gson gson) {
    apiPrefix = config.getString("api.url");
    apiAppName = config.hasPath("api.app") ? config.getString("api.app") : null;
    this.gson = gson;
  }

  public MinuteBars getHistoMinute(String base, String terms, LocalDateTime toUtc, int limit) {

    long now = LocalDateTime.now(ZoneId.of("UTC")).truncatedTo(ChronoUnit.MINUTES).toEpochSecond(ZoneOffset.UTC);
    long end = toUtc.truncatedTo(ChronoUnit.MINUTES).toEpochSecond(ZoneOffset.UTC);
    if (now == end) {
      end -= 60;
    }
    LinkedHashMap<String, String> params = new LinkedHashMap<>();
    params.put("fsym", base);
    params.put("tsym", terms);
    params.put("toTs", String.valueOf(end));
    params.put("limit", String.valueOf(limit));
    if (apiAppName != null) {
      params.put("extraParams", apiAppName);
    }
    String res = RestGetJson.request(apiPrefix + HISTO_MINUTE, params);
    LOGGER.info("HistoMinute result: {}", res);
    if (res == null) {
      return MinuteBars.fail("null response");
    } else {
      return gson.fromJson(res, MinuteBars.class);
    }
  }
}
