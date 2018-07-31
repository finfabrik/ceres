package com.blokaly.ceres.common;

import com.google.common.base.Objects;

import java.io.Serializable;

public class Tuple<L, R> implements Serializable {

  public static final Tuple<?, ?> NULL_TUPLE = new Tuple<>(null, null);

  private final L left;
  private final R right;

  public Tuple(L left, R right) {
    this.left = left;
    this.right = right;
  }

  public L getLeft() {
    return left;
  }

  public R getRight() {
    return right;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Tuple<?, ?> tuple = (Tuple<?, ?>) o;
    return Objects.equal(left, tuple.left) &&
        Objects.equal(right, tuple.right);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(left, right);
  }

  @Override
  public String toString() {
    return "Tuple{" + left + ", " + right + '}';
  }

}
