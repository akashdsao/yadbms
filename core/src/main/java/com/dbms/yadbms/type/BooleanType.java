package com.dbms.yadbms.type;

import static com.dbms.yadbms.type.Limits.BOOLEAN_NULL;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import jdk.jshell.spi.ExecutionControl.NotImplementedException;

/** BooleanType - implementation of BOOLEAN SQL type in Java */
public class BooleanType extends Type {

  public BooleanType() {
    super(TypeId.BOOLEAN);
  }

  @Override
  public CmpBool compareEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return (left.asBoolean() == right.castAs(TypeId.BOOLEAN).asBoolean())
        ? CmpBool.CmpTrue
        : CmpBool.CmpFalse;
  }

  @Override
  public CmpBool compareNotEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return (left.asBoolean() != right.castAs(TypeId.BOOLEAN).asBoolean())
        ? CmpBool.CmpTrue
        : CmpBool.CmpFalse;
  }

  @Override
  public CmpBool compareLessThan(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return (left.asBoolean() ? 1 : 0) < (right.asBoolean() ? 1 : 0)
        ? CmpBool.CmpTrue
        : CmpBool.CmpFalse;
  }

  @Override
  public CmpBool compareLessThanEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return (left.asBoolean() ? 1 : 0) <= (right.asBoolean() ? 1 : 0)
        ? CmpBool.CmpTrue
        : CmpBool.CmpFalse;
  }

  @Override
  public CmpBool compareGreaterThan(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return (left.asBoolean() ? 1 : 0) > (right.asBoolean() ? 1 : 0)
        ? CmpBool.CmpTrue
        : CmpBool.CmpFalse;
  }

  @Override
  public CmpBool compareGreaterThanEquals(Value left, Value right) {
    if (left.isNull() || right.isNull()) return CmpBool.CmpNull;
    return (left.asBoolean() ? 1 : 0) >= (right.asBoolean() ? 1 : 0)
        ? CmpBool.CmpTrue
        : CmpBool.CmpFalse;
  }

  @Override
  public boolean isInlined(Value val) {
    return true;
  }

  @Override
  public String toString(Value val) {
    if (val.isNull()) return "boolean_null";
    return val.asBoolean() ? "true" : "false";
  }

  @Override
  public void serializeTo(Value val, byte[] storage, int offset) {
    ByteBuffer buffer = ByteBuffer.wrap(storage).order(ByteOrder.BIG_ENDIAN);

    if (val.isNull()) {
      buffer.put((byte) -1); // sentinel for NULL
    } else {
      buffer.put((byte) (val.asBoolean() ? 1 : 0));
    }
  }

  @Override
  public Value deserializeFrom(byte[] storage) {
    ByteBuffer buffer = ByteBuffer.wrap(storage).order(ByteOrder.BIG_ENDIAN);
    byte b = buffer.get();

    if (b == -1) {
      return new Value(TypeId.BOOLEAN, BOOLEAN_NULL); // NULL
    }
    return new Value(TypeId.BOOLEAN, b == 1);
  }

  @Override
  public Value copy(Value val) {
    if (val.isNull()) return new Value(TypeId.BOOLEAN);
    return new Value(TypeId.BOOLEAN, val.asBoolean());
  }

  @Override
  public Value castAs(Value val, TypeId targetType) {
    switch (targetType) {
      case BOOLEAN:
        return copy(val);
      case VARCHAR:
        if (val.isNull()) {
          return new Value(TypeId.VARCHAR, (String) null);
        }
        return new Value(TypeId.VARCHAR, toString(val));
      default:
        throw new IllegalArgumentException("BOOLEAN is not coercible to " + targetType);
    }
  }

  @Override
  public byte[] getData(Value val) throws NotImplementedException {
    throw new NotImplementedException("");
  }

  @Override
  public int getStorageSize(Value val) {
    return 1; // always 1 byte
  }

  // Arithmetic not supported for BOOLEAN
  @Override
  public Value add(Value l, Value r) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Value subtract(Value l, Value r) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Value multiply(Value l, Value r) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Value divide(Value l, Value r) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Value modulo(Value l, Value r) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Value min(Value l, Value r) {
    return (compareLessThanEquals(l, r) == CmpBool.CmpTrue) ? l : r;
  }

  @Override
  public Value max(Value l, Value r) {
    return (compareGreaterThanEquals(l, r) == CmpBool.CmpTrue) ? l : r;
  }

  @Override
  public Value sqrt(Value v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Value operateNull(Value l, Value r) {
    return new Value(TypeId.BOOLEAN);
  }

  @Override
  public boolean isZero(Value v) {
    return !v.asBoolean();
  }
}
