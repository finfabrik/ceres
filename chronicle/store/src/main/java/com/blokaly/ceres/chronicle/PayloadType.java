package com.blokaly.ceres.chronicle;

import java.util.HashMap;
import java.util.Map;

public enum PayloadType {
  BEGIN('B'), END('E'), JSON('J');

  private static final Map<Character, PayloadType> lookup = new HashMap<Character, PayloadType>();

  static {
    for (PayloadType type : PayloadType.values()) {
      lookup.put(type.getType(), type);
    }
  }

  private final char type;
  private PayloadType(char type) {
    this.type = type;
  }

  public static PayloadType parse(byte type) {
    Character key = (char) type;
    PayloadType payloadType = lookup.get(key);
    if (payloadType == null) {
      throw new IllegalArgumentException("unknown type " + key);
    }
    return payloadType;
  }

  public char getType() {
    return this.type;
  }

  public byte byteType() {
    return (byte)this.type;
  }


}
