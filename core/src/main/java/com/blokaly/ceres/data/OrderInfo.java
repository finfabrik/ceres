package com.blokaly.ceres.data;

import com.blokaly.ceres.common.DecimalNumber;

import java.util.HashMap;
import java.util.Map;

public interface OrderInfo {

  Side side();

  DecimalNumber getPrice();

  DecimalNumber getQuantity();

  enum Side {
    BUY("B"), SELL("S"), UNKNOWN("U");

    private static Map<String, Side> valueMap;

    static {
      valueMap = new HashMap<>();
      for (Side side : Side.values()) {
        valueMap.put(side.shortName, side);
      }
    }

    private final String shortName;

    private Side(String shortName) {
      this.shortName = shortName;
    }

    public static Side parse(String side) {
      if (side == null || side.length() == 0) {
        return UNKNOWN;
      }

      try {
        return Side.valueOf(side.toUpperCase());
      } catch (Exception ex) {
        return UNKNOWN;
      }
    }

    public static Side parseByField(String shortname) {
      return valueMap.get(shortname);
    }

    public String shortName() {
      return shortName;
    }
  }
}
