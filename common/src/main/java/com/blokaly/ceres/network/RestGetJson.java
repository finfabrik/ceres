package com.blokaly.ceres.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;

public class RestGetJson {
  private static final Logger LOGGER = LoggerFactory.getLogger(RestGetJson.class);
  private static final String UTF_8 = "UTF-8";

  public static String request(String url) {
    return request(makeUrl(url, null));
  }

  public static String request(URL url) {
    if (url == null) {
      return null;
    }
    LOGGER.info("Requesting json: {}", url.toString());
    try (HttpReader reader = new HttpReader((HttpURLConnection)url.openConnection())) {
      HttpURLConnection conn = reader.getConnection();
      conn.setRequestMethod("GET");
      conn.setRequestProperty("Accept", "application/json");
      return reader.read();
    } catch (Exception ex) {
      LOGGER.error("Error requesting json, url:" + url.toString(), ex);
      return null;
    }
  }

  public static String request(String endpoint, Map<String, String> params) {
    URL url = makeUrl(endpoint, params);
    if (url != null) {
      return request(url);
    } else {
      return null;
    }
  }

  public static URL makeUrl(String endpointUrl, Map<String, String> params) {

    StringBuilder sb = new StringBuilder(endpointUrl);
    try {
      if (params != null && !params.isEmpty()) {
        boolean isFirst = true;
        for (Map.Entry<String, String> param : params.entrySet()) {
          if (isFirst) {
            sb.append('?');
            isFirst = false;
          } else {
            sb.append('&');
          }

          sb.append(URLEncoder.encode(param.getKey(), UTF_8));
          sb.append("=");
          sb.append(URLEncoder.encode(param.getValue(), UTF_8));
        }
      }
      return new URL(sb.toString());
    } catch (Exception ex) {
      LOGGER.error("Error generating url for " + endpointUrl, ex);
      return null;
    }
  }
}
