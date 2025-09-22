package com.dbms.yadbms.type;

public final class Limits {
  public static final byte INT8_MIN = Byte.MIN_VALUE;
  public static final byte INT8_MAX = Byte.MAX_VALUE;
  public static final short INT16_MIN = Short.MIN_VALUE;
  public static final short INT16_MAX = Short.MAX_VALUE;
  public static final int INT32_MIN = Integer.MIN_VALUE;
  public static final int INT32_MAX = Integer.MAX_VALUE;
  public static final long INT64_MIN = Long.MIN_VALUE;
  public static final long INT64_MAX = Long.MAX_VALUE;

  public static final double DECIMAL_MIN = -Double.MAX_VALUE;
  public static final double DECIMAL_MAX = Double.MAX_VALUE;

  public static final long TIMESTAMP_MIN = 0L; // or define epoch-based
  public static final long TIMESTAMP_MAX = Long.MAX_VALUE;

  public static final byte INT8_NULL = Byte.MIN_VALUE; // -128
  public static final short INT16_NULL = Short.MIN_VALUE; // -32768
  public static final int INT32_NULL = Integer.MIN_VALUE; // -2147483648
  public static final long INT64_NULL = Long.MIN_VALUE; // -9223372036854775808
  public static final double DECIMAL_NULL = Double.NaN; // Double.NEGATIVE_INFINITY
  public static final byte BOOLEAN_NULL = Byte.MIN_VALUE; // -128
  public static final int NULL_VALUE = Integer.MAX_VALUE;
}
