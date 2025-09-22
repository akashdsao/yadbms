package com.dbms.yadbms.type;

public class ValueFactory {

  public static Value getBooleanValue(boolean b) {
    return new Value(TypeId.BOOLEAN, b);
  }

  public static Value getIntegerValue(int i) {
    return new Value(TypeId.INTEGER, i);
  }

  public static Value getBigIntValue(long l) {
    return new Value(TypeId.BIGINT, l);
  }

  public static Value getSmallIntValue(short s) {
    return new Value(TypeId.SMALLINT, (int) s);
  }

  public static Value getTinyIntValue(byte b) {
    return new Value(TypeId.TINYINT, (int) b);
  }

  public static Value getDecimalValue(double d) {
    return new Value(TypeId.DECIMAL, d);
  }

  public static Value getVarcharValue(String s) {
    return new Value(TypeId.VARCHAR, s);
  }

  public static Value getVectorValue(double[] v) {
    return new Value(TypeId.VECTOR, v);
  }

  public static Value getNullValue(TypeId t) {
    return new Value(t);
  }

  public static Value castAsTinyInt(Value v) {
    if (v.isNull()) return getNullValue(TypeId.TINYINT);
    switch (v.getTypeId()) {
      case TINYINT:
        return v;
      case SMALLINT:
        short s = v.asInt().shortValue();
        if (s < Limits.INT8_MIN || s > Limits.INT8_MAX)
          throw new ArithmeticException("Out of range for TINYINT");
        return getTinyIntValue((byte) s);
      case INTEGER:
        int i = v.asInt();
        if (i < Limits.INT8_MIN || i > Limits.INT8_MAX)
          throw new ArithmeticException("Out of range for TINYINT");
        return getTinyIntValue((byte) i);
      case BIGINT:
        long l = v.asBigInt();
        if (l < Limits.INT8_MIN || l > Limits.INT8_MAX)
          throw new ArithmeticException("Out of range for TINYINT");
        return getTinyIntValue((byte) l);
      case DECIMAL:
        double d = v.asDecimal();
        if (d < Limits.INT8_MIN || d > Limits.INT8_MAX)
          throw new ArithmeticException("Out of range for TINYINT");
        return getTinyIntValue((byte) d);
      case VARCHAR:
        return getTinyIntValue(Byte.parseByte(v.asString()));
      default:
        throw new IllegalArgumentException("Cannot cast " + v.getTypeId() + " to TINYINT");
    }
  }

  public static Value castAsSmallInt(Value v) {
    if (v.isNull()) return getNullValue(TypeId.SMALLINT);
    switch (v.getTypeId()) {
      case SMALLINT:
        return v;
      case TINYINT:
        return getSmallIntValue(v.asInt().shortValue());
      case INTEGER:
        int i = v.asInt();
        if (i < Limits.INT16_MIN || i > Limits.INT16_MAX)
          throw new ArithmeticException("Out of range for SMALLINT");
        return getSmallIntValue((short) i);
      case BIGINT:
        long l = v.asBigInt();
        if (l < Limits.INT16_MIN || l > Limits.INT16_MAX)
          throw new ArithmeticException("Out of range for SMALLINT");
        return getSmallIntValue((short) l);
      case DECIMAL:
        double d = v.asDecimal();
        if (d < Limits.INT16_MIN || d > Limits.INT16_MAX)
          throw new ArithmeticException("Out of range for SMALLINT");
        return getSmallIntValue((short) d);
      case VARCHAR:
        return getSmallIntValue(Short.parseShort(v.asString()));
      default:
        throw new IllegalArgumentException("Cannot cast " + v.getTypeId() + " to SMALLINT");
    }
  }

  public static Value castAsInteger(Value v) {
    if (v.isNull()) return getNullValue(TypeId.INTEGER);
    switch (v.getTypeId()) {
      case INTEGER:
        return v;
      case TINYINT:
      case SMALLINT:
        return getIntegerValue(v.asInt());
      case BIGINT:
        long l = v.asBigInt();
        if (l < Limits.INT32_MIN || l > Limits.INT32_MAX)
          throw new ArithmeticException("Out of range for INTEGER");
        return getIntegerValue((int) l);
      case DECIMAL:
        double d = v.asDecimal();
        if (d < Limits.INT32_MIN || d > Limits.INT32_MAX)
          throw new ArithmeticException("Out of range for INTEGER");
        return getIntegerValue((int) d);
      case VARCHAR:
        return getIntegerValue(Integer.parseInt(v.asString()));
      case BOOLEAN:
        return getIntegerValue(v.asBoolean() ? 1 : 0);
      default:
        throw new IllegalArgumentException("Cannot cast " + v.getTypeId() + " to INTEGER");
    }
  }

  public static Value castAsBigInt(Value v) {
    if (v.isNull()) return getNullValue(TypeId.BIGINT);
    switch (v.getTypeId()) {
      case BIGINT:
        return v;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return getBigIntValue(v.asInt());
      case DECIMAL:
        double d = v.asDecimal();
        if (d < Limits.INT64_MIN || d > Limits.INT64_MAX)
          throw new ArithmeticException("Out of range for BIGINT");
        return getBigIntValue((long) d);
      case VARCHAR:
        return getBigIntValue(Long.parseLong(v.asString()));
      case BOOLEAN:
        return getBigIntValue(v.asBoolean() ? 1 : 0);
      default:
        throw new IllegalArgumentException("Cannot cast " + v.getTypeId() + " to BIGINT");
    }
  }

  public static Value castAsDecimal(Value v) {
    if (v.isNull()) return getNullValue(TypeId.DECIMAL);
    switch (v.getTypeId()) {
      case DECIMAL:
        return v;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return getDecimalValue(v.asInt().doubleValue());
      case BIGINT:
        return getDecimalValue(v.asBigInt().doubleValue());
      case VARCHAR:
        return getDecimalValue(Double.parseDouble(v.asString()));
      case BOOLEAN:
        return getDecimalValue(v.asBoolean() ? 1.0 : 0.0);
      default:
        throw new IllegalArgumentException("Cannot cast " + v.getTypeId() + " to DECIMAL");
    }
  }

  public static Value castAsVarchar(Value v) {
    if (v.isNull()) return getNullValue(TypeId.VARCHAR);
    return getVarcharValue(v.toString());
  }

  public static Value castAsBoolean(Value v) {
    if (v.isNull()) return getNullValue(TypeId.BOOLEAN);
    switch (v.getTypeId()) {
      case BOOLEAN:
        return v;
      case INTEGER:
        return getBooleanValue(v.asInt() != 0);
      case BIGINT:
        return getBooleanValue(v.asBigInt() != 0);
      case DECIMAL:
        return getBooleanValue(v.asDecimal() != 0.0);
      case VARCHAR:
        String s = v.asString().toLowerCase();
        if (s.equals("true") || s.equals("1") || s.equals("t")) return getBooleanValue(true);
        if (s.equals("false") || s.equals("0") || s.equals("f")) return getBooleanValue(false);
        throw new IllegalArgumentException("Invalid boolean string: " + s);
      default:
        throw new IllegalArgumentException("Cannot cast " + v.getTypeId() + " to BOOLEAN");
    }
  }
}
