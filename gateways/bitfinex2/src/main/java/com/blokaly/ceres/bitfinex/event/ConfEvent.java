package com.blokaly.ceres.bitfinex.event;

public class ConfEvent extends AbstractEvent {

  private static final int DEC_S = 8;
  private static final int TIME_S = 32;
  private static final int TIMESTAMP = 32768;
  private static final int SEQ_ALL = 65536;
  private static final int CHECKSUM = 131072;

  private int flags;
  private String status;

  public ConfEvent() {
    super("conf");
  }

  public boolean isOK() {
    return "OK".equalsIgnoreCase(status);
  }

  public ConfEvent enableDecimalString() {
    flags += DEC_S;
    return this;
  }

  public ConfEvent enableTimeString() {
    flags += TIME_S;
    return this;
  }

  public ConfEvent enableTimeInMillis() {
    flags += TIMESTAMP;
    return this;
  }

  public ConfEvent enableSeqence() {
    flags += SEQ_ALL;
    return this;
  }

  public ConfEvent enableChecksum() {
    flags += CHECKSUM;
    return this;
  }

  @Override
  public String toString() {
    return "ConfEvent{" +
        "flags=" + flags +
        ", status=" + status +
        '}';
  }
}
