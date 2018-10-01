package com.blokaly.ceres.orderbook;

import com.blokaly.ceres.common.DecimalNumber;
import com.blokaly.ceres.data.IdBasedOrderInfo;
import com.blokaly.ceres.data.MarketDataIncremental;
import com.blokaly.ceres.data.MarketDataSnapshot;
import com.blokaly.ceres.data.OrderInfo;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class OrderBasedOrderBook implements OrderBook<IdBasedOrderInfo>, TopOfBook {
  private static final Logger LOGGER = LoggerFactory.getLogger(OrderBasedOrderBook.class);
  private final String symbol;
  private final String key;
  private final NavigableMap<DecimalNumber, PriceLevel> bids = Maps.newTreeMap(Comparator.<DecimalNumber>reverseOrder());
  private final NavigableMap<DecimalNumber, PriceLevel> asks = Maps.newTreeMap();
  private final Map<String, PriceLevel> levelByOrderId = Maps.newHashMap();
  private final Map<DecimalNumber, DeltaLevel> delta = Maps.newHashMap();
  private long lastSequence;
  private long lastUpdTime;

  public OrderBasedOrderBook(String symbol, String key) {
    this.symbol = symbol;
    this.key = key;
    this.lastSequence = 0;
    this.lastUpdTime = 0;
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
    levelByOrderId.clear();
    lastSequence = 0;
  }


  @Override
  public void processSnapshot(MarketDataSnapshot<IdBasedOrderInfo> snapshot) {
    LOGGER.debug("processing snapshot");
    clear();
    snapshot.getBids().forEach(this::processNewOrder);
    snapshot.getAsks().forEach(this::processNewOrder);
    lastSequence = snapshot.getSequence();
  }

  @Override
  public void processIncrementalUpdate(MarketDataIncremental<IdBasedOrderInfo> incremental) {

    if (lastSequence == 0) {
      return;
    }

    long sequence = incremental.getSequence();
    if (sequence <= lastSequence) {
      return;
    }

    LOGGER.debug("processing market data: {}", incremental);

    switch (incremental.type()) {
      case NEW:
        incremental.orderInfos().forEach(this::processNewOrder);
        break;
      case UPDATE:
        incremental.orderInfos().forEach(this::processUpdateOrder);
        break;
      case DONE:
        incremental.orderInfos().forEach(this::processDoneOrder);
        break;
      default:
        LOGGER.debug("Unknown type of market data: {}", incremental);
    }

    lastSequence = sequence;
  }

  public Collection<DeltaLevel> getDelta() {
    return delta.values();
  }

  public void clearDelta() {
    delta.clear();
  }

  public long getLastUpdateTime() {
    return lastUpdTime;
  }

  public void setLastUpdateTime(long time) {
    this.lastUpdTime = time;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("OrderBook{");
    sb.append(symbol).append(", bids=[");
    bids.values().forEach(level -> {sb.append(" ").append(level);});
    sb.append(" ], asks=[");
    asks.values().forEach(level -> {sb.append(" ").append(level);});
    sb.append(" ]}");
    return sb.toString();
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

  private NavigableMap<DecimalNumber, PriceLevel> sidedLevels(OrderInfo.Side side) {
    if (side == null || side == OrderInfo.Side.UNKNOWN) {
      return null;
    }

    return side== OrderInfo.Side.BUY ? bids : asks;
  }

  private void processNewOrder(IdBasedOrderInfo order) {
    OrderInfo.Side side = order.side();
    NavigableMap<DecimalNumber, PriceLevel> levels = sidedLevels(side);
    DecimalNumber price = order.getPrice();
    String orderId = order.getId();
    PriceLevel level = levels.get(price);
    if (level == null) {
      level = new PriceLevel(price);
      levels.put(price, level);
    }
    level.addOrChange(orderId, order.getQuantity());
    levelByOrderId.put(orderId, level);
    delta.put(level.price, new DeltaLevel(level, side, MarketDataIncremental.Type.NEW));
  }

  private void processDoneOrder(IdBasedOrderInfo order) {
    String orderId = order.getId();
    PriceLevel level = levelByOrderId.remove(orderId);
    if (level == null) {
      return;
    }

    boolean emptyLevel = level.remove(orderId);
    if (emptyLevel) {
      NavigableMap<DecimalNumber, PriceLevel> levels = sidedLevels(order.side());
      levels.remove(level.getPrice());
    }
    delta.put(level.price, new DeltaLevel(level, order.side(), MarketDataIncremental.Type.DONE));
  }

  private void processUpdateOrder(IdBasedOrderInfo order) {
    String orderId = order.getId();
    PriceLevel level = levelByOrderId.get(orderId);
    if (level == null) {
      processNewOrder(order);
    } else {
      level.addOrChange(orderId, order.getQuantity());
      delta.put(level.price, new DeltaLevel(level, order.side(), MarketDataIncremental.Type.UPDATE));
    }

  }

  public static final class PriceLevel implements OrderBook.Level {
    private final DecimalNumber price;
    private final Map<String, DecimalNumber> quantityByOrderId = Maps.newHashMap();
    private DecimalNumber total;

    private PriceLevel(DecimalNumber price) {
      this.price = price;
      this.total = DecimalNumber.ZERO;
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

    private void addOrChange(String orderId, DecimalNumber quantity) {
      total = total.plus(quantity).minus(quantityByOrderId.getOrDefault(orderId, DecimalNumber.ZERO));
      quantityByOrderId.put(orderId, quantity);
    }

    private boolean remove(String orderId) {
      DecimalNumber current = quantityByOrderId.remove(orderId);
      if (current != null) {
        total = total.minus(current);
      }

      if (quantityByOrderId.isEmpty()) {
        return true;
      } else {
        if (total.signum() <= 0) {
          quantityByOrderId.clear();
          return true;
        } else {
          return false;
        }
      }
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
