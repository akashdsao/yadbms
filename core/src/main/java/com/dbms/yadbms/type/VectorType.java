package com.dbms.yadbms.type;

import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/** VectorType - stores an array of doubles. Variable-length, not inlined. */
public class VectorType extends Type {

  public VectorType() {
    super(TypeId.VECTOR);
  }

  // ---------- Data access ----------

  @Override
  public byte[] getData(Value val) {
    if (val.isNull()) return null;
    double[] vec = val.asVector();
    ByteBuffer buffer = ByteBuffer.allocate(vec.length * Double.BYTES).order(ByteOrder.BIG_ENDIAN);
    for (double d : vec) buffer.putDouble(d);
    return buffer.array();
  }

  @Override
  public int getStorageSize(Value val) {
    if (val.isNull()) return 0;
    return val.asVector().length * Double.BYTES;
  }

  public double[] getVector(Value val) {
    return val.asVector();
  }

  // ---------- Comparisons (not supported) ----------

  @Override
  public CmpBool compareEquals(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "VECTOR comparison not supported");
  }

  @Override
  public CmpBool compareNotEquals(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "VECTOR comparison not supported");
  }

  @Override
  public CmpBool compareLessThan(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "VECTOR comparison not supported");
  }

  @Override
  public CmpBool compareLessThanEquals(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "VECTOR comparison not supported");
  }

  @Override
  public CmpBool compareGreaterThan(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "VECTOR comparison not supported");
  }

  @Override
  public CmpBool compareGreaterThanEquals(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "VECTOR comparison not supported");
  }

  @Override
  public Value min(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "VECTOR min not supported");
  }

  @Override
  public Value max(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "VECTOR max not supported");
  }

  // ---------- Debug ----------

  @Override
  public String toString(Value val) {
    if (val.isNull()) return "vector_null";
    return Arrays.toString(val.asVector());
  }

  // ---------- Storage ----------

  @Override
  public void serializeTo(Value val, byte[] storage, int offset) {
    if (val.isNull()) {
      ByteBuffer.wrap(storage, offset, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(-1);
      return;
    }
    double[] vec = val.asVector();
    ByteBuffer.wrap(storage, offset, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(vec.length);
    ByteBuffer buf =
        ByteBuffer.wrap(storage, offset + Integer.BYTES, vec.length * Double.BYTES)
            .order(ByteOrder.BIG_ENDIAN);
    for (double d : vec) buf.putDouble(d);
  }

  @Override
  public Value deserializeFrom(byte[] storage) {
    ByteBuffer buffer = ByteBuffer.wrap(storage).order(ByteOrder.BIG_ENDIAN);
    int len = buffer.getInt();
    if (len == -1) {
      return new Value(TypeId.VECTOR, (double[]) null);
    }
    int count = len / Double.BYTES;
    double[] vec = new double[count];
    for (int i = 0; i < count; i++) vec[i] = buffer.getDouble();
    return new Value(TypeId.VECTOR, vec);
  }

  @Override
  public Value copy(Value val) {
    if (val.isNull()) return new Value(TypeId.VECTOR, (double[]) null);
    return new Value(TypeId.VECTOR, val.asVector());
  }

  // ---------- Casting ----------

  @Override
  public Value castAs(Value val, TypeId targetType) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "VECTOR cast not supported");
  }

  // ---------- Required overrides ----------

  @Override
  public boolean isZero(Value val) {
    return false;
  }

  @Override
  public Value add(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Add not supported for VECTOR");
  }

  @Override
  public Value subtract(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Subtract not supported for VECTOR");
  }

  @Override
  public Value multiply(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Multiply not supported for VECTOR");
  }

  @Override
  public Value divide(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Divide not supported for VECTOR");
  }

  @Override
  public Value modulo(Value left, Value right) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Modulo not supported for VECTOR");
  }

  @Override
  public Value sqrt(Value val) {
    throw new DBException(ErrorType.UNSUPPORTED_OPERATION, "Sqrt not supported for VECTOR");
  }

  @Override
  public Value operateNull(Value left, Value right) {
    return new Value(TypeId.VECTOR, (double[]) null);
  }

  @Override
  public boolean isInlined(Value val) {
    return false;
  }
}
