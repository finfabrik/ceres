package com.blokaly.ceres.cryptocompare.api;

import com.google.gson.*;

import java.lang.reflect.Type;

public class MinuteBars {
  private final boolean success;
  private final String message;
  private final long timeFrom;
  private final long timeTo;
  private final Bar[] bars;

  private MinuteBars(boolean success, String message, long timeFrom, long timeTo, Bar... bars) {
    this.success = success;
    this.message = message;
    this.timeFrom = timeFrom;
    this.timeTo = timeTo;
    this.bars = bars;
  }

  public static MinuteBars success(long from, long to, Bar... bars) {
    return new MinuteBars(true, null, from, to, bars);
  }

  public static MinuteBars fail(String message) {
    return new MinuteBars(false, message, 0, 0);
  }

  public boolean isSuccess() {
    return success;
  }

  public String getMessage() {
    return message;
  }

  public long getTimeFrom() {
    return timeFrom;
  }

  public long getTimeTo() {
    return timeTo;
  }

  public Bar[] getBars() {
    return bars;
  }

  public static class Bar {
    private long time;
    private double open;
    private double high;
    private double low;
    private double close;
    private double volumefrom;
    private double volumeto;

    public long getTime() {
      return time;
    }

    public double getOpen() {
      return open;
    }

    public double getHigh() {
      return high;
    }

    public double getLow() {
      return low;
    }

    public double getClose() {
      return close;
    }

    public double getVolumeFrom() {
      return volumefrom;
    }

    public double getVolumeTo() {
      return volumeto;
    }
  }

  public static class EventAdapter implements JsonDeserializer<MinuteBars> {
    @Override
    public MinuteBars deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
      JsonObject res = json.getAsJsonObject();
      if (res.has("Response") && "Success".equalsIgnoreCase(res.get("Response").getAsString())) {
        JsonArray data = res.get("Data").getAsJsonArray();
        MinuteBars.Bar[] bars = context.deserialize(data, MinuteBars.Bar[].class);
        long fromEpochSec = res.get("TimeFrom").getAsLong();
        long toEpochSec = res.get("TimeTo").getAsLong();
        return MinuteBars.success(fromEpochSec, toEpochSec, bars);
      } else {
        String msg = res.has("Message") ? res.get("Message").getAsString() : res.toString();
        return MinuteBars.fail(msg);
      }
    }
  }
}
