package com.blokaly.ceres.orderbook;

import com.blokaly.ceres.common.DecimalNumber;
import com.blokaly.ceres.data.MarketDataIncremental;
import com.blokaly.ceres.data.MarketDataSnapshot;
import com.blokaly.ceres.data.OrderInfo;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;

public class PriceBasedOrderBook implements OrderBook<OrderInfo>, TopOfBook {

  private static final Logger LOGGER = LoggerFactory.getLogger(PriceBasedOrderBook.class);
  private final String symbol;
  private final String key;
  private final NavigableMap<DecimalNumber, PriceLevel> bids = Maps.newTreeMap(Comparator.<DecimalNumber>reverseOrder());
  private final NavigableMap<DecimalNumber, PriceLevel> asks = Maps.newTreeMap();
  private final Map<DecimalNumber, DeltaLevel> delta = Maps.newHashMap();
  private long lastSequence;

  public PriceBasedOrderBook(String symbol, String key) {
    this.symbol = symbol;
    this.key = key;
    this.lastSequence = 0;
  }

  @Override
  public String getSymbol() {
    return symbol;
  }

  @Override
  public long getLastSequence() {
    return lastSequence;
  }

  @Override
  public Collection<? extends Level> getBids() {
    return bids.values();
  }

  @Override
  public Collection<? extends Level> getAsks() {
    return asks.values();
  }

  public Collection<? extends Level> getReverseBids() {
    return bids.descendingMap().values();
  }

  public Collection<? extends Level> getReverseAsks() {
    return asks.descendingMap().values();
  }

  @Override
  public void clear() {
    bids.clear();
    asks.clear();
    lastSequence = 0;
  }

  public boolean isInitialized() {
    return lastSequence>0;
  }

  @Override
  public void processSnapshot(MarketDataSnapshot<OrderInfo> snapshot) {
    LOGGER.debug("processing snapshot: {}", snapshot);
    clear();
    snapshot.getBids().forEach(this::processUpdate);
    snapshot.getAsks().forEach(this::processUpdate);
    lastSequence = snapshot.getSequence();
  }

  private void processUpdate(OrderInfo order) {
    OrderInfo.Side side = order.side();
    NavigableMap<DecimalNumber, PriceLevel> levels = sidedLevels(side);
    PriceLevel level = new PriceLevel(order.getPrice(), order.getQuantity());
    if (level.getQuantity().compareTo(DecimalNumber.ZERO) > 0) {
      levels.put(level.price, level);
      delta.put(level.price, new DeltaLevel(level, side, MarketDataIncremental.Type.UPDATE));
    } else {
      processDeletion(order);
    }
  }

  private NavigableMap<DecimalNumber, PriceLevel> sidedLevels(OrderInfo.Side side) {
    if (side == null || side == OrderInfo.Side.UNKNOWN) {
      return null;
    }

    return side == OrderInfo.Side.BUY ? bids : asks;
  }

  private void processDeletion(OrderInfo order) {
    OrderInfo.Side side = order.side();
    NavigableMap<DecimalNumber, PriceLevel> levels = sidedLevels(side);
    PriceLevel level = levels.remove(order.getPrice());
    if (level != null) {
      delta.put(level.price, new DeltaLevel(level, side, MarketDataIncremental.Type.DONE));
    }

  }

  @Override
  public void processIncrementalUpdate(MarketDataIncremental<OrderInfo> incremental) {
    long sequence = incremental.getSequence();
    if (sequence < lastSequence) {
      return;
    }

    LOGGER.debug("processing market data: {}", incremental);
    delta.clear();
    switch (incremental.type()) {
      case NEW:
      case UPDATE:
        incremental.orderInfos().forEach(this::processUpdate);
        break;
      case DONE:
        incremental.orderInfos().forEach(this::processDeletion);
        break;
      default:
        LOGGER.debug("Unknown type of market data: {}", incremental);
    }

    lastSequence = sequence;
  }

  public Collection<PriceBasedOrderBook.DeltaLevel> getDelta() {
    return delta.values();
  }

  @Override
  public String getKey() {
    return key;
  }

  @Override
  public Entry[] topOfBids(int depth) {
    Entry[] entries = new Entry[depth];
    int idx = 0;
    for (Map.Entry<DecimalNumber, PriceLevel> entry : bids.entrySet()) {
      if (idx >= depth) {
        break;
      }
      entries[idx++] = wrapPriceLevel(entry);
    }
    return entries;
  }

  @Override
  public Entry[] topOfAsks(int depth) {
    Entry[] entries = new Entry[depth];
    int idx = 0;
    for (Map.Entry<DecimalNumber, PriceLevel> entry : asks.entrySet()) {
      if (idx >= depth) {
        break;
      }
      entries[idx++] = wrapPriceLevel(entry);
    }
    return entries;
  }

  private Entry wrapPriceLevel(Map.Entry<DecimalNumber, PriceLevel> entry) {
    if (entry == null) {
      return null;
    } else {
      PriceLevel level = entry.getValue();
      return new Entry(level.getPrice().toString(), level.getQuantity().toString());
    }
  }

  public static final class PriceLevel implements OrderBook.Level {

    private final DecimalNumber price;
    private final DecimalNumber total;

    private PriceLevel(DecimalNumber price, DecimalNumber total) {
      this.price = price;
      this.total = total;
    }

    @Override
    public DecimalNumber getPrice() {
      return price;
    }

    @Override
    public DecimalNumber getQuantity() {
      return total;
    }

    @Override
    public String toString() {
      return "[" + price.toString() + "," + total.toString() + "]";
    }

  }

  public static final class DeltaLevel implements OrderBook.Level {
    private final PriceLevel level;
    private final OrderInfo.Side side;
    private final MarketDataIncremental.Type type;

    private DeltaLevel(PriceLevel level, OrderInfo.Side side, MarketDataIncremental.Type type) {
      this.level = level;
      this.side = side;
      this.type = type;
    }

    public OrderInfo.Side getSide() {
      return side;
    }

    public MarketDataIncremental.Type getType() {
      return type;
    }

    @Override
    public DecimalNumber getPrice() {
      return level.getPrice();
    }

    @Override
    public DecimalNumber getQuantity() {
      return level.getQuantity();
    }
  }
}
