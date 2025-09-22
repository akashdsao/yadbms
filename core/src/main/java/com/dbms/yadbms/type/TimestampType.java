package com.dbms.yadbms.type;

import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * TimestampType - SQL TIMESTAMP type. Internally stored as a long (microseconds and encoded
 * components).
 */
public class TimestampType extends Type {

  public TimestampType() {
    super(TypeId.TIMESTAMP);
  }

  // ---------- Comparisons ----------

  @Override
  public CmpBool compareEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return CmpBool.from(left.asBigInt() == right.asBigInt());
  }

  @Override
  public CmpBool compareNotEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return CmpBool.from(left.asBigInt() != right.asBigInt());
  }

  @Override
  public CmpBool compareLessThan(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return CmpBool.from(left.asBigInt() < right.asBigInt());
  }

  @Override
  public CmpBool compareLessThanEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return CmpBool.from(left.asBigInt() <= right.asBigInt());
  }

  @Override
  public CmpBool compareGreaterThan(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return CmpBool.from(left.asBigInt() > right.asBigInt());
  }

  @Override
  public CmpBool compareGreaterThanEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return CmpBool.from(left.asBigInt() >= right.asBigInt());
  }

  // ---------- Aggregates ----------

  @Override
  public Value min(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    return (left.compareLessThan(right) == CmpBool.CmpTrue) ? left.copy() : right.copy();
  }

  @Override
  public Value max(Value left, Value right) {
    if (left.isNull() || right.isNull()) return left.operateNull(right);
    return (left.compareGreaterThanEquals(right) == CmpBool.CmpTrue) ? left.copy() : right.copy();
  }

  // ---------- Debug / String ----------

  @Override
  public String toString(Value val) {
    if (val.isNull()) return "timestamp_null";

    long tm = val.asBigInt(); // encoded timestamp
    long micro = tm % 1_000_000;
    tm /= 1_000_000;

    long second = tm % 100000;
    int sec = (int) (second % 60);
    second /= 60;
    int min = (int) (second % 60);
    second /= 60;
    int hour = (int) (second % 24);

    tm /= 100000;
    int year = (int) (tm % 10000);

    tm /= 10000;
    int tz = (int) (tm % 27) - 12;
    tm /= 27;

    int day = (int) (tm % 32);
    tm /= 32;
    int month = (int) tm;

    String dateStr =
        String.format(
            "%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, min, sec, micro);
    String zone = String.format("%s%02d", tz >= 0 ? "+" : "-", Math.abs(tz));

    return dateStr + zone;
  }

  // ---------- Storage ----------

  @Override
  public void serializeTo(Value val, byte[] storage, int offset) {
    long v = val.asBigInt(); // stored as long
    ByteBuffer.wrap(storage, offset, Long.BYTES).order(ByteOrder.BIG_ENDIAN).putLong(v);
  }

  @Override
  public Value deserializeFrom(byte[] storage) {
    long v = ByteBuffer.wrap(storage).order(ByteOrder.BIG_ENDIAN).getLong();
    return new Value(TypeId.TIMESTAMP, v);
  }

  @Override
  public Value copy(Value val) {
    if (val.isNull()) return new Value(TypeId.TIMESTAMP);
    return new Value(TypeId.TIMESTAMP, val.asBigInt());
  }

  // ---------- Casting ----------

  @Override
  public Value castAs(Value val, TypeId targetType) {
    if (val.isNull()) return new Value(targetType);

    switch (targetType) {
      case TIMESTAMP:
        return this.copy(val);
      case VARCHAR:
        return new Value(TypeId.VARCHAR, this.toString(val));
      default:
        throw new DBException(
            ErrorType.INVALID_OPERATION, "TIMESTAMP not coercible to " + targetType);
    }
  }

  // ---------- Required overrides (no-op for TIMESTAMP) ----------

  @Override
  public Value add(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Add not supported for TIMESTAMP");
  }

  @Override
  public Value subtract(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Subtract not supported for TIMESTAMP");
  }

  @Override
  public Value multiply(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Multiply not supported for TIMESTAMP");
  }

  @Override
  public Value divide(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Divide not supported for TIMESTAMP");
  }

  @Override
  public Value modulo(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Modulo not supported for TIMESTAMP");
  }

  @Override
  public Value sqrt(Value val) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Sqrt not supported for TIMESTAMP");
  }

  @Override
  public Value operateNull(Value left, Value right) {
    return new Value(TypeId.TIMESTAMP);
  }

  @Override
  public boolean isZero(Value val) {
    return false;
  }

  @Override
  public boolean isInlined(Value val) {
    return true;
  }

  @Override
  public byte[] getData(Value val) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN);
    buffer.putLong(val.asBigInt());
    return buffer.array();
  }

  @Override
  public int getStorageSize(Value val) {
    return Long.BYTES; // 8
  }
}
