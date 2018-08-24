package com.blokaly.ceres.influxdb.ringbuffer;

import com.lmax.disruptor.EventFactory;
import org.influxdb.dto.Point;
import org.influxdb.impl.Preconditions;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class PointBuilderFactory implements EventFactory<PointBuilderFactory.BatchedPointBuilder> {

  @Override
  public PointBuilderFactory.BatchedPointBuilder newInstance() {
    return new PointBuilderFactory.BatchedPointBuilder();
  }

  public static final class BatchedPointBuilder {
    private String measurement;
    private Long time;
    private TimeUnit precision;
    private final Map<String, String> tags = new TreeMap<>();
    private final Map<String, Object> fields = new TreeMap<>();

    public BatchedPointBuilder measurement(String measurement) {
      this.measurement = measurement;
      return this;
    }

    public BatchedPointBuilder tag(String tagName, String value) {
      Objects.requireNonNull(tagName, "tagName");
      Objects.requireNonNull(value, "value");
      if (!tagName.isEmpty() && !value.isEmpty()) {
        this.tags.put(tagName, value);
      }

      return this;
    }

    public BatchedPointBuilder tag(Map<String, String> tagsToAdd) {
      Iterator var2 = tagsToAdd.entrySet().iterator();

      while(var2.hasNext()) {
        Map.Entry<String, String> tag = (Map.Entry)var2.next();
        this.tag((String)tag.getKey(), (String)tag.getValue());
      }

      return this;
    }

    public BatchedPointBuilder addField(String field, boolean value) {
      this.fields.put(field, value);
      return this;
    }

    public BatchedPointBuilder addField(String field, long value) {
      this.fields.put(field, value);
      return this;
    }

    public BatchedPointBuilder addField(String field, double value) {
      this.fields.put(field, value);
      return this;
    }

    public BatchedPointBuilder addField(String field, Number value) {
      this.fields.put(field, value);
      return this;
    }

    public BatchedPointBuilder addField(String field, String value) {
      Objects.requireNonNull(value, "value");
      this.fields.put(field, value);
      return this;
    }

    public BatchedPointBuilder fields(Map<String, Object> fieldsToAdd) {
      this.fields.putAll(fieldsToAdd);
      return this;
    }

    public BatchedPointBuilder time(long timeToSet, TimeUnit precisionToSet) {
      Objects.requireNonNull(precisionToSet, "precisionToSet");
      this.time = timeToSet;
      this.precision = precisionToSet;
      return this;
    }

    public boolean hasFields() {
      return !this.fields.isEmpty();
    }

    public Point build() {
      Preconditions.checkNonEmptyString(this.measurement, "measurement");
      Preconditions.checkPositiveNumber(this.fields.size(), "fields size");

      Point.Builder builder = Point.measurement(this.measurement);
      builder.tag(tags);
      builder.fields(fields);
      if (this.time != null) {
        builder.time(this.time, this.precision);
      }
      return builder.build();
    }

    public BatchedPointBuilder reset() {
      this.measurement = null;
      this.time = null;
      this.precision = null;
      this.tags.clear();
      this.fields.clear();
      return this;
    }
  }
}
