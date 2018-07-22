package com.blokaly.ceres.coinmarketcap;

import com.blokaly.ceres.network.RestGetJson;
import com.google.gson.Gson;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class TickerRequester {
  private static final Logger LOGGER = LoggerFactory.getLogger(TickerRequester.class);
  private static final String COINMARKETCAP_TICKER_API = "https://api.coinmarketcap.com/v1/ticker/?limit=0";

  public TickerEvent[] request(Gson gson) {
    String res = RestGetJson.request(COINMARKETCAP_TICKER_API);
    if (res != null) {
      return gson.fromJson(res, TickerEvent[].class);
    } else {
      return new TickerEvent[0];
    }
  }
}
