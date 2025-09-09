package com.dbms.yadbms.config;

import static com.dbms.yadbms.common.utils.Constants.DEFAULT_FRAME_ID;

/**
 * FrameId represents a unique identifier for a frame in the buffer pool. It encapsulates an Integer
 * value that identifies the frame. The FrameId is used to manage frames in the buffer pool,
 * allowing for efficient access and replacement.
 */
public class FrameId {

  private final Integer value;

  public FrameId(Integer value) {
    if (value <= 0) {
      value = DEFAULT_FRAME_ID;
    }
    this.value = value;
  }

  public static FrameId store(Integer value) {
    return new FrameId(value);
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

  /** Two FrameId objects are considered equal if their Integer values are equal. */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FrameId)) return false;
    FrameId frameId = (FrameId) o;
    return value.equals(frameId.value);
  }

  /**
   * Returns the hash code for this FrameId. The hash code is based on the Integer value of the
   * FrameId.
   *
   * @return the hash code
   */
  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
