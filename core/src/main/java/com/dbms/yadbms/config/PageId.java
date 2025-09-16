package com.dbms.yadbms.config;

import java.util.Objects;

/**
 * Represents a unique identifier for a page in the database. A PageId is an immutable object that
 * encapsulates an Integer value. It is used to identify pages in the buffer pool and disk storage.
 *
 * <p>A PageId is considered valid if its value is non-negative.
 */
public class PageId {

  private final Integer value;

  public PageId(Integer value) {
    this.value = value;
  }

  public static PageId store(Integer value) {
    return new PageId(value);
  }

  /**
   * Returns the Integer value of this FrameId.
   *
   * @return the Integer value
   */
  public int getValue() {
    return value;
  }

  /**
   * Returns the String representation of this FrameId.
   *
   * @return the String representation
   */
  public String toString() {
    return Integer.toString(value);
  }

  /**
   * Checks if this FrameId is valid. A FrameId is considered valid if its value is not null and is
   * non-negative.
   *
   * @return true if valid, false otherwise
   */
  public Boolean isValid() {
    return value >= 0;
  }

  /** Two PageId objects are considered equal if their Integer values are equal. */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PageId)) return false;
    PageId pageId = (PageId) o;
    return value.equals(pageId.value);
  }

  /**
   * Returns the hash code of this PageId. The hash code is based on the Integer value of the
   * PageId.
   */
  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
