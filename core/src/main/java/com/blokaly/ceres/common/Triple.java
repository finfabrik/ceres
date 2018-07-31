package com.blokaly.ceres.common;

import com.google.common.base.Objects;

import java.io.Serializable;

public class Triple<L, M, R> implements Serializable {

  public static final Triple<?, ?, ?> NULL_TRIPLE = new Triple<>(null, null, null);

  private final L left;
  private final M middle;
  private final R right;

  public Triple(L left, M mid, R right) {
    this.left = left;
    this.middle = mid;
    this.right = right;
  }

  public L getLeft() {
    return left;
  }

  public M getMiddle() {
    return middle;
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
    Triple<?, ?, ?> triple = (Triple<?, ?, ?>) o;
    return Objects.equal(left, triple.left) &&
        Objects.equal(middle, triple.middle) &&
        Objects.equal(right, triple.right);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(left, middle, right);
  }

  @Override
  public String toString() {
    return "Triple{" + left + ", " + middle + ", " + right + '}';
  }
}
